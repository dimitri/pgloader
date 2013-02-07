;;;
;;; Tools to handle MySQL data fetching
;;;

(defpackage #:pgloader.mysql
  (:use #:cl)
  (:import-from #:pgloader
		#:*loader-kernel*
		#:*myconn-host*
		#:*myconn-user*
		#:*myconn-pass*)
  (:export #:map-rows
	   #:copy-from
	   #:list-databases
	   #:list-tables
	   #:export-all-tables
	   #:export-import-database
	   #:stream-mysql-table-in-pgsql
	   #:stream-database-tables))

(in-package :pgloader.mysql)

;;;
;;; MySQL tools connecting to a database
;;;
(defun list-databases (&key
			 (host *myconn-host*)
			 (user *myconn-user*)
			 (pass *myconn-pass*))
  "Connect to a local database and get the database list"
  (cl-mysql:connect :host host :user user :password pass)
  (unwind-protect
       (mapcan #'identity (caar (cl-mysql:query "show databases")))
    (cl-mysql:disconnect)))

(defun list-tables (dbname
		    &key
		      (host *myconn-host*)
		      (user *myconn-user*)
		      (pass *myconn-pass*))
  "Return a flat list of all the tables names known in given DATABASE"
  (cl-mysql:connect :host host :user user :password pass)

  (unwind-protect
       (progn
	 (cl-mysql:use dbname)
	 ;; that returns a pretty weird format, process it
	 (mapcan #'identity (caar (cl-mysql:list-tables))))
    ;; free resources
    (cl-mysql:disconnect)))

;;;
;;; Map a function to each row extracted from MySQL
;;;
(defun map-rows (dbname table-name process-row-fn
		 &key
		   (host *myconn-host*)
		   (user *myconn-user*)
		   (pass *myconn-pass*))
  "Extract MySQL data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (cl-mysql:connect :host host :user user :password pass)

  (unwind-protect
       (progn
	 ;; Ensure we're talking utf-8 and connect to DBNAME in MySQL
	 (cl-mysql:query "SET NAMES 'utf8'")
	 (cl-mysql:query "SET character_set_results = utf8;")
	 (cl-mysql:use dbname)

	 (let* ((sql (format nil "SELECT * FROM ~a;" table-name))
		(q   (cl-mysql:query sql :store nil))
		(rs  (cl-mysql:next-result-set q)))
	   (declare (ignore rs))

	   ;; Now fetch MySQL rows directly in the stream
	   (loop
	      for row = (cl-mysql:next-row q :type-map (make-hash-table))
	      while row
	      counting row into count
	      do (funcall process-row-fn row)
	      finally (return count))))

    ;; free resources
    (cl-mysql:disconnect)))

;;;
;;; Use mysql-map-rows and pgsql-text-copy-format to fill in a CSV file on
;;; disk with MySQL data in there.
;;;
(defun copy-from (dbname table-name filename
		  &key
		    date-columns
		    (host *myconn-host*)
		    (user *myconn-user*)
		    (pass *myconn-pass*))
  "Extrat data from MySQL in PostgreSQL COPY TEXT format"
  (with-open-file (text-file filename
			     :direction :output
			     :if-exists :supersede
			     :external-format :utf8)
    (map-rows dbname table-name
	      (lambda (row)
		(pgloader.pgsql:format-row text-file
					   row
					   :date-columns date-columns))
	      :host host
	      :user user
	      :pass pass)))

;;;
;;; MySQL bulk export to file, in PostgreSQL COPY TEXT format
;;;
(defun export-all-tables (dbname
			  &key
			    only-tables
			    (host *myconn-host*)
			    (user *myconn-user*)
			    (pass *myconn-pass*))
  "Export MySQL tables into as many TEXT files, in the PostgreSQL COPY format"
  (let ((pgtables (pgloader.pgsql:list-tables dbname))
	(total-count 0)
	(total-seconds 0))
    (pgloader.utils:report-header)
    (loop
       for table-name in (list-tables dbname
				      :host host
				      :user user
				      :pass pass)
       for filename = (pgloader.csv:get-pathname dbname table-name)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgloader.utils:report-table-name table-name)
	 (multiple-value-bind (result seconds)
	     (pgloader.utils:timing
	      (copy-from dbname table-name filename
			 :date-columns (pgloader.pgsql:get-date-columns
					table-name pgtables)))
	   (when result
	     (incf total-count result)
	     (incf total-seconds seconds)
	     (pgloader.utils:report-results result seconds)))
       finally
	 (pgloader.utils:report-footer "Total export time"
				       total-count total-seconds)
	 (return (values total-count (float total-seconds))))))


;;;
;;; Copy data for a target database from files in the PostgreSQL COPY TEXT
;;; format
;;;
(defun export-import-database (dbname
			       &key
				 (truncate t)
				 only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  ;; get the list of tables and have at it
  (let ((mysql-tables (list-tables dbname))
	(total-count 0)
	(total-seconds 0))
    (pgloader.utils:report-header)
    (loop
       for (table-name . date-columns) in (pgloader.pgsql:list-tables dbname)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgloader.utils:report-table-name table-name)

	 (if (member table-name mysql-tables :test #'equal)
	     (multiple-value-bind (result seconds)
		 (pgloader.utils:timing
		  (let ((filename
			 (pgloader.csv:get-pathname dbname table-name)))
		    ;; export from MySQL to file
		    (copy-from dbname table-name filename
			       :date-columns date-columns)
		    ;; import the file to PostgreSQL
		    (pgloader.pgsql:copy-from-file dbname table-name filename
						   :truncate truncate)))
	       (when result
		 (incf total-count result)
		 (incf total-seconds seconds)
		 (pgloader.utils:report-results result seconds)))
	     ;; not a known mysql table
	     (format t " skip, unknown table in MySQL database~%"))
       finally
	 (pgloader.utils:report-footer "Total export+import time"
				       total-count total-seconds)
	 (return (values total-count (float total-seconds))))))


;;;
;;; Direct "stream" in between mysql fetching of results and PostgreSQL COPY
;;; protocol
;;;
(defun stream-mysql-table-in-pgsql (dbname table-name
				    &key
				    truncate
				    date-columns)
  "Connect in parallel to MySQL and PostgreSQL and stream the data."
  (let* ((lp:*kernel* *loader-kernel*)
	 (channel     (lp:make-channel))
	 (dataq       (lq:make-queue 4096)))
    ;; have a task fill MySQL data in the queue
    (lp:submit-task
     channel (lambda ()
	       (list :mysql
		     (pgloader.queue:map-push-queue
		      dataq
		      #'map-rows dbname table-name))))

    ;; and start another task to push that data from the queue to PostgreSQL
    (lp:submit-task
     channel
     (lambda ()
       (list :pgsql
	     (multiple-value-list
	      (pgloader.pgsql:copy-from-queue dbname table-name dataq
					      :truncate truncate
					      :date-columns date-columns)))))

    ;; now wait until both the tasks are over
    (loop
       for tasks below 2
       collect (lp:receive-result channel) into counts
       finally (return (cadr (assoc :pgsql counts))))))

;;;
;;; Work on all tables for given database
;;;
(defun stream-database-tables (dbname &key (truncate t) only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  ;; get the list of tables and have at it
  (let ((mysql-tables (list-tables dbname))
	(total-count 0)
	(total-errors 0)
	(total-seconds 0))
    (pgloader.utils:report-header)
    (loop
       for (table-name . date-columns) in (pgloader.pgsql:list-tables dbname)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgloader.utils:report-table-name table-name)

	 (if (member table-name mysql-tables :test #'equal)
	     (multiple-value-bind (result seconds)
		 (pgloader.utils:timing
		   (destructuring-bind (rows errors)
		       (stream-mysql-table-in-pgsql dbname table-name
						    :truncate truncate
						    :date-columns date-columns)
		     (incf total-count rows)
		     (incf total-errors errors)
		     (list rows errors)))
	       ;; time to report
	       (destructuring-bind (rows errors) result
		 (incf total-seconds seconds)
		 (pgloader.utils:report-results rows errors seconds)))
	     ;; not a known mysql table
	     (format t "skip, unknown table in MySQL database~%"))
       finally
	 (pgloader.utils:report-footer "Total streaming time"
				       total-count total-errors total-seconds)
	 (return (values total-count (float total-seconds))))))


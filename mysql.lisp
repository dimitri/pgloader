;;;
;;; Tools to handle MySQL data fetching
;;;

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
(defun map-rows (dbname table-name
		 &key
		   process-row-fn
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
;;; Use map-rows and pgsql-text-copy-format to fill in a CSV file on disk
;;; with MySQL data in there.
;;;
(defun copy-to (dbname table-name filename
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
	      :process-row-fn
	      (lambda (row)
		(pgloader.pgsql:format-row text-file row
					   :date-columns date-columns))
	      :host host
	      :user user
	      :pass pass)))

;;;
;;; MySQL bulk export to file, in PostgreSQL COPY TEXT format
;;;
(defun export-database (dbname
			&key
			  only-tables
			  (host *myconn-host*)
			  (user *myconn-user*)
			  (pass *myconn-pass*))
  "Export MySQL tables into as many TEXT files, in the PostgreSQL COPY format"
  (let ((pgtables (pgloader.pgsql:list-tables dbname)))
    (setf *state* (pgloader.utils:make-pgstate))
    (report-header)
    (loop
       for table-name in (list-tables dbname
				      :host host
				      :user user
				      :pass pass)
       for filename = (pgloader.csv:get-pathname dbname table-name)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgstate-add-table *state* dbname table-name)
	 (report-table-name table-name)
	 (multiple-value-bind (rows secs)
	     (timing
	      ;; load data
	      (let ((date-cols
		     (pgloader.pgsql:get-date-columns table-name pgtables)))
		(copy-to dbname table-name filename :date-columns date-cols)))
	   ;; update and report stats
	   (pgstate-incf *state* table-name :read rows :secs secs)
	   (report-pgtable-stats *state* table-name))
       finally
	 (report-pgstate-stats *state* "Total export time"))))

;;;
;;; Copy data from a target database into files in the PostgreSQL COPY TEXT
;;; format, then load those files. Useful mainly to compare timing with the
;;; direct streaming method. If you need to pre-process the files, use
;;; export-database, do the extra processing, then use
;;; pgloader.pgsql:copy-from-file on each file.
;;;
(defun export-import-database (dbname
			       &key
				 (truncate t)
				 only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  ;; get the list of tables and have at it
  (let ((mysql-tables (list-tables dbname)))
    (setf *state* (pgloader.utils:make-pgstate))
    (report-header)
    (loop
       for (table-name . date-columns) in (pgloader.pgsql:list-tables dbname)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgstate-add-table *state* dbname table-name)
	 (report-table-name table-name)

	 (if (member table-name mysql-tables :test #'equal)
	     (multiple-value-bind (res secs)
		 (timing
		  (let* ((filename
			 (pgloader.csv:get-pathname dbname table-name))
			 (read
			  ;; export from MySQL to file
			  (copy-to dbname table-name filename
				   :date-columns date-columns)))
		    ;; import the file to PostgreSQL
		    (pgloader.pgsql:copy-from-file dbname
						   table-name
						   filename
						   :truncate truncate)))
	       (declare (ignore res))
	       (pgstate-incf *state* table-name :secs secs)
	       (report-pgtable-stats *state* table-name))
	     ;; not a known mysql table
	     (format t " skip, unknown table in MySQL database~%"))
       finally
	 (report-pgstate-stats *state* "Total export+import time"))))

;;;
;;; Export MySQL data to our lparallel data queue. All the work is done in
;;; other basic layers, simple enough function.
;;;
(defun copy-to-queue (dbname table-name dataq)
  "Copy data from MySQL table DBNAME.TABLE-NAME into queue DATAQ"
  (let ((read
	 (pgloader.queue:map-push-queue dataq #'map-rows dbname table-name)))
    (pgstate-incf *state* table-name :read read)))

;;;
;;; Direct "stream" in between mysql fetching of results and PostgreSQL COPY
;;; protocol
;;;
(defun stream-table (dbname table-name
		     &key
		       truncate
		       date-columns)
  "Connect in parallel to MySQL and PostgreSQL and stream the data."
  (let* ((lp:*kernel* *loader-kernel*)
	 (channel     (lp:make-channel))
	 (dataq       (lq:make-queue 4096)))
    (lp:submit-task channel (lambda ()
			      ;; this function update :read stats
			      (copy-to-queue dbname table-name dataq)))

    ;; and start another task to push that data from the queue to PostgreSQL
    (lp:submit-task
     channel
     (lambda ()
       ;; this function update :rows stats
       (pgloader.pgsql:copy-from-queue dbname table-name dataq
				       :truncate truncate
				       :date-columns date-columns)))

    ;; now wait until both the tasks are over
    (loop for tasks below 2 do (lp:receive-result channel))))

;;;
;;; Work on all tables for given database
;;;
(defun stream-database (dbname &key (truncate t) only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  ;; get the list of tables and have at it
  (let ((mysql-tables (list-tables dbname)))
    (setf *state* (pgloader.utils:make-pgstate))
    (report-header)
    (loop
       for (table-name . date-columns) in (pgloader.pgsql:list-tables dbname)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgstate-add-table *state* dbname table-name)
	 (report-table-name table-name)

	 (if (member table-name mysql-tables :test #'equal)
	     (multiple-value-bind (res secs)
		 (timing
		  ;; this will care about updating stats in *state*
		  (stream-table dbname table-name
				:truncate truncate
				:date-columns date-columns))
	       ;; set the timing we just measured
	       (declare (ignore res))
	       (pgstate-incf *state* table-name :secs secs)
	       (report-pgtable-stats *state* table-name))
	     ;; not a known mysql table
	     (format t "skip, unknown table in MySQL database~%"))
       finally
	 (report-pgstate-stats *state* "Total streaming time"))))


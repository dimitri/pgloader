;;;; galaxya-loader.lisp

(in-package #:galaxya-loader)

;;;
;;; Parameters you might want to change
;;;
(defparameter *loader-kernel* (lp:make-kernel 2)
  "lparallel kernel to use for loading data in parallel")

(defparameter *myconn-host* "gala3.galaxya.fr")
(defparameter *myconn-user* "pg1")
(defparameter *myconn-pass* "AFmhKERxD9PVjgQD")

(setq *myconn-host* "localhost"
      *myconn-user* "debian-sys-maint"
      *myconn-pass* "vtmMI04yBZlFprYm")

(defparameter *pgconn*
  '("gdb" "none" "localhost" :port 5432)
  "Connection string to the local database")

(defparameter *csv-path-root*
  (merge-pathnames "csv/" (user-homedir-pathname)))

(defun get-csv-pathname (dbname table-name)
  "return where to find the file"
  (make-pathname
   :directory (pathname-directory
	       (merge-pathnames (format nil "~a/" dbname) *csv-path-root*))
   :name table-name
   :type "csv"))

;;;
;;; PostgreSQL Utilities
;;;
(defun get-connection-string (dbname)
  (cons dbname *pgconn*))

(defun get-database-list ()
  "connect to a local database and get the database list"
  (pomo:with-connection
      (get-connection-string "postgres")
    (loop for (dbname) in (pomo:query
			   "select datname
                              from pg_database
                             where datname !~ 'postgres|template'")
	 collect dbname)))

(defun get-table-list (dbname)
  "Return an alist of tables names and list of columns to pay attention to."
  (pomo:with-connection
      (get-connection-string dbname)

    (loop for (relname colarray) in (pomo:query "
select relname, array_agg(case when typname in ('date', 'timestamptz')
                               then attnum end
                          order by attnum)
      from pg_class c
           join pg_namespace n on n.oid = c.relnamespace
           left join pg_attribute a on c.oid = a.attrelid
           join pg_type t on t.oid = a.atttypid
     where c.relkind = 'r'
           and attnum > 0
           and n.nspname = 'public'
  group by relname
")
       collect (cons relname (loop
				for attnum across colarray
				unless (eq attnum :NULL)
				collect attnum)))))

(defun pgsql-truncate-table (dbname table-name)
  "Truncate given TABLE-NAME in database DBNAME"
  (pomo:with-connection (get-connection-string dbname)
    (pomo:execute (format nil "truncate ~a;" table-name))))

(defun load-data-in-pgsql-text-format (dbname table-name filename
				       &key
					 (truncate t))
  "Load data from clean CSV file to PostgreSQL"
  (with-open-file
      ;; we just ignore files that don't exist
      (input filename
	     :direction :input
	     :if-does-not-exist nil)
    (when input
      (when truncate
	(pgsql-truncate-table dbname table-name))

      ;; read csv in the file and push it directly through the db writer
      ;; in COPY streaming mode
      (let* ((conspec  (remove :port (get-connection-string dbname)))
	     (stream
	      (cl-postgres:open-db-writer conspec table-name nil)))

	(unwind-protect
	     (loop
		for line = (read-line input nil)
		for row = (mapcar (lambda (x)
				    (if (string= "\\N" x) :null x))
				  (sq:split-sequence #\Tab line))
		while line
		counting line into count
		do (cl-postgres:db-write-row stream row)
		finally (return count))
	  (cl-postgres:close-db-writer stream))))))

;;;
;;; Export data from MySQL as a COPY TEXT file then import that file into
;;; the destination PostgreSQL table.
;;;
(defun load-single-table-using-file (dbname table-name
				     &key
				     (truncate t)
				     date-columns)
  "Load a single table: export data from MySQL to CSV file then load that in PG"
  (let ((filename (get-csv-pathname dbname table-name)))
    (mysql-copy-text-format dbname table-name
			    filename
			    :date-columns date-columns)
    (load-data-in-pgsql-text-format dbname table-name filename
				    :truncate truncate)))

;;;
;;; Let's go parallel, with a queue to communicate data
;;;
(defun load-data-from-queue-to-pgsql (dbname table-name dataq
				      &key
					(truncate t)
					date-columns)
  "Fetch data from the QUEUE until we see :end-of-data"
  (when truncate (pgsql-truncate-table dbname table-name))

  (let* ((conspec  (remove :port (get-connection-string dbname)))
	 (stream
	  (cl-postgres:open-db-writer conspec table-name nil)))
    (unwind-protect
	 (loop
	    for row = (lq:pop-queue dataq)
	    until (eq row :end-of-data)
	    counting row into count
	    do (let ((pgrow
		      (pgsql-reformat-row row :date-columns date-columns)))
		 (cl-postgres:db-write-row stream pgrow))
	    finally (return (list :pgsql count)))
      (cl-postgres:close-db-writer stream))))

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
    (lp:submit-task channel
		    (lambda ()
		      (prog1
			  (list :mysql
				(mysql-map-rows
				 dbname table-name
				 (lambda (row)
				   (lq:push-queue row dataq)))))
		      (lq:push-queue :end-of-data dataq)))

    ;; and start another task to push that data from the queue to PostgreSQL
    (lp:submit-task channel
		    (lambda ()
		      (load-data-from-queue-to-pgsql dbname table-name dataq
						     :truncate truncate
						     :date-columns date-columns)))

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
  (let ((mysql-tables (list-tables-in-mysql-db dbname))
	(total-count 0)
	(total-seconds 0))
    (format t "~&~30@a  ~9@a  ~9@a" "table name" "rows" "time")
    (format t "~&------------------------------  ---------  ---------")
    (loop
       for (table-name . date-columns) in (get-table-list dbname)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (format t "~&~30@a  " table-name)

	 (if (member table-name mysql-tables :test #'equal)
	     (multiple-value-bind (result seconds)
		 (timing
		   (stream-mysql-table-in-pgsql dbname table-name
						:truncate truncate
						:date-columns date-columns))
	       (when result
		 (incf total-count result)
		 (incf total-seconds seconds)
		 (format t "~9@a  ~9@a"
			 result (format-interval seconds nil))))
	     ;; not a known mysql table
	     (format t "skip, unknown table in MySQL database~%"))
       finally
	 (format t "~&------------------------------  ---------  ---------")
	 (format t "~&~30@a  ~9@a  ~9@a" "Total streaming time"
		 total-count (format-interval total-seconds nil))
	 (return (values total-count (float total-seconds))))))

(defun load-database-tables-from-file (dbname &key (truncate t) only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  ;; get the list of tables and have at it
  (let ((mysql-tables (list-tables-in-mysql-db dbname))
	(total-count 0)
	(total-seconds 0))
    (format t "~&~30@a  ~9@a  ~9@a" "table name" "rows" "time")
    (format t "~&------------------------------  ---------  ---------")
    (loop
       for (table-name . date-columns) in (get-table-list dbname)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (format t "~&~30@a  " table-name)

	 (if (member table-name mysql-tables :test #'equal)
	     (multiple-value-bind (result seconds)
		 (timing
		   (load-single-table-using-file dbname table-name
						 :truncate truncate
						 :date-columns date-columns))
	       (when result
		 (incf total-count result)
		 (incf total-seconds seconds)
		 (format t "~9@a  ~9@a"
			 result (format-interval seconds nil))))
	     ;; not a known mysql table
	     (format t " skip, unknown table in MySQL database~%"))
       finally
	 (format t "~&------------------------------  ---------  ---------")
	 (format t "~&~30@a  ~9@a  ~9@a" "Total export+import time"
		 total-count (format-interval total-seconds nil))
	 (return (values total-count (float total-seconds))))))

(defun load-all-databases ()
  (pomo:with-connection
      (get-connection-string "postgres")

    ;; get the list of databases and have at it
    (loop
       for dbname in (get-database-list)
       do
	 (format t "~&DATABASE: ~a ..." dbname)
	 (multiple-value-bind (result seconds)
	     (timing
	       (load-database-tables dbname))
	   (format t " ~d rows in ~f secs~%" result seconds)))))

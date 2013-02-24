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
;;; Tools to get MySQL table and columns definitions and transform them to
;;; PostgreSQL CREATE TABLE statements, and run those.
;;;
(defun list-all-columns (dbname
			 &key
			   (host *myconn-host*)
			   (user *myconn-user*)
			   (pass *myconn-pass*))
  "Get the list of MySQL column names per table."
  (cl-mysql:connect :host host :user user :password pass)

  (unwind-protect
       (progn
	 (loop
	    with schema = nil
	    for (table-name . coldef)
	    in
	      (caar (cl-mysql:query (format nil "
  select table_name, column_name,
         data_type, column_type, column_default,
         is_nullable, extra
    from information_schema.columns
   where table_schema = '~a'
order by table_name, ordinal_position" dbname)))
	    do
	      (let ((entry (assoc table-name schema :test 'equal)))
		(if entry
		    (push coldef (cdr entry))
		    (push (cons table-name (list coldef)) schema)))
	    finally (return schema)))

    ;; free resources
    (cl-mysql:disconnect)))

(defun parse-column-typemod (column-type)
  "Given int(7), returns the number 7."
  (parse-integer (nth 1
		      (sq:split-sequence-if (lambda (c) (member c '(#\( #\))))
					    column-type
					    :remove-empty-subseqs t))))

(defun cast (dtype ctype nullable default extra)
  "Convert a MySQL datatype to a PostgreSQL datatype.

DYTPE is the MySQL data_type and CTYPE the MySQL column_type, for example
that would be int and int(7) or varchar and varchar(25).
"
  (let* ((pgtype
	  (cond
	    ((and (string= dtype "int")
		  (string= extra "auto_increment"))
	     (if (< (parse-column-typemod ctype) 10) "serial" "bigserial"))

	    ;; this time it can't be an auto_increment
	    ((string= dtype "int")
	     (if (< (parse-column-typemod ctype) 10) "int" "bigint"))

	    ;; no support for varchar(x) yet, mostly not needed
	    ((string= dtype "varchar") "text")

	    ((string= dtype "datetime") "timestamptz")

	    (t dtype)))

	 ;; forget about stupid defaults
	 (default (cond ((and (string= dtype "datetime")
			      (string= default "0000-00-00 00:00:00"))
			 nil)

			((and (string= dtype "date")
			      (string= default "0000-00-00"))
			 nil)

			(t default)))

	 ;; force to accept NULLs when we remove stupid default values
	 (nullable (or (and (not (string= extra "auto_increment"))
			    (not default))
		       (not (string= nullable "NO")))))

    ;; now format the column definition and return it
    (format nil
	    "~a~:[ not null~;~]~:[~; default ~a~]"
	    pgtype nullable default default)))

(defun get-create-table (table-name cols)
  "Return a PostgreSQL CREATE TABLE statement from MySQL columns"
  (with-output-to-string (s)
    (format s "CREATE TABLE ~a ~%(~%" table-name)
    (loop
       for ((name dtype ctype default nullable extra) . last?) on (reverse cols)
       for pg-coldef = (cast dtype ctype nullable default extra)
       do (format s "  ~a ~22t ~a~:[~;,~]~%" name pg-coldef last?))
    (format s ");~%")))

(defun get-drop-table-if-exists (table-name)
  "Return the PostgreSQL DROP TABLE IF EXISTS statement for TABLE-NAME."
  (format nil "DROP TABLE IF EXISTS ~a;~%" table-name))

(defun get-pgsql-create-tables (dbname &key include-drop)
  "Return the list of CREATE TABLE statements to run against PostgreSQL"
  (loop
     for (table-name . cols) in (list-all-columns dbname)
     when include-drop collect (get-drop-table-if-exists table-name)
     collect (get-create-table table-name cols)))

(defun pgsql-create-tables (dbname &key include-drop)
  "Create all MySQL tables in database dbname in PostgreSQL"
  (loop
     for nb-tables from 0
     for sql in (get-pgsql-create-tables dbname :include-drop include-drop)
     do (pgloader.pgsql:execute dbname sql)
     finally (return nb-tables)))

;;;
;;; Tools to get MySQL indexes definitions and transform them to PostgreSQL
;;; indexes definitions, and run those statements.
;;;
(defun list-all-indexes (dbname
			 &key
			   (host *myconn-host*)
			   (user *myconn-user*)
			   (pass *myconn-pass*))
  "Get the list of MySQL index definitions per table."
  (cl-mysql:connect :host host :user user :password pass)

  (unwind-protect
       (progn
	 (cl-mysql:use "information_schema")
	 (loop
	    for (table-name index-name non-unique cols)
	    in (caar (cl-mysql:query (format nil "
  SELECT table_name, index_name, non_unique,
         GROUP_CONCAT(column_name order by seq_in_index)
    FROM information_schema.statistics
   WHERE table_schema = '~a'
GROUP BY table_name, index_name;" dbname)))
	    collect (list table-name
			  index-name
			  (not (= 1 non-unique))
			  (sq:split-sequence #\, cols))))

    ;; free resources
    (cl-mysql:disconnect)))

(defun get-pgsql-index-def (table-name index-name unique cols)
  "Return a PostgreSQL CREATE INDEX statement as a string."
  (cond ((string= index-name "PRIMARY")
	 (format nil
		 "ALTER TABLE ~a ADD PRIMARY KEY (~{~a~^, ~});"
		 table-name cols))

	(t
	 (format nil
		 "CREATE INDEX ~a_idx ON ~a (~{~a~^, ~});"
		 index-name table-name cols))))

(defun get-drop-index-if-exists (table-name index-name)
  "Return the DROP INDEX statement for PostgreSQL"
  (cond ((string= index-name "PRIMARY")
	 (format nil
		 "ALTER TABLE ~a DROP CONSTRAINT ~a_pkey;"
		 table-name table-name))

	(t
	 (format nil
		 "DROP INDEX IF EXISTS ~a_idx;" index-name))))

(defun get-pgsql-create-indexes (indexes &key include-drop)
  "Return the CREATE INDEX statements from given INDEXES definitions."
  (loop
     for (table-name index-name unique cols) in indexes
     append (append
	     ;; use append to avoid collecting NIL entries
	     (when (and include-drop (not (string= index-name "PRIMARY")))
	       ;; no need to alter table drop constraint, when include-drop
	       ;; is true we just did drop the table and created it again
	       ;; anyway
	       (list (get-drop-index-if-exists table-name index-name)))
	     (list (get-pgsql-index-def table-name index-name unique cols)))))

(defun pgsql-create-indexes (dbname &key include-drop)
  "Create all MySQL tables in database dbname in PostgreSQL"
  (loop
     for nb-indexes from 0
     for sql in (get-pgsql-create-indexes (list-all-indexes dbname)
					  :include-drop include-drop)
     do (pgloader.pgsql:execute dbname sql)
     finally (return nb-indexes)))

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
(defmacro with-silent-timing (state dbname table-name &body body)
  "Wrap body with timing and stats reporting."
  `(progn
     (pgstate-add-table ,state ,dbname ,table-name)
     (report-table-name ,table-name)

     (multiple-value-bind (res secs)
	 (timing
	  ;; we don't want to see the warnings
	  ;; but we still want to capture the result
	  (let ((res))
	    (with-output-to-string (s)
	      (let ((*standard-output* s) (*error-output* s))
		(setf res ,@body)))
	    res))
       (pgstate-incf ,state ,table-name :rows res :secs secs))
     (report-pgtable-stats ,state ,table-name)))

(defun stream-database (dbname
			&key
			  (create-tables nil)
			  (include-drop nil)
			  (truncate t)
			  only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  ;; get the list of tables and have at it
  (let ((mysql-tables (list-tables dbname)))
    (setf *state* (pgloader.utils:make-pgstate))
    (report-header)

    ;; if asked, first drop/create the tables on the PostgreSQL side
    (when create-tables
      (with-silent-timing *state* dbname "CREATE TABLES"
	(pgsql-create-tables dbname :include-drop include-drop)))

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
	 (when create-tables
	   ;; now we have to create the indexes and primary keys
	   (with-silent-timing *state* dbname "CREATE INDEXES"
	     (pgsql-create-indexes dbname :include-drop include-drop)))
	 ;; and report the total time spent on the operation
	 (report-pgstate-stats *state* "Total streaming time"))))


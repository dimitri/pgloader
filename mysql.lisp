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
	    finally
	      ;; we did push, we need to reverse here
	      (return (loop
			 for (name . cols) in schema
			 collect (cons name (reverse cols))))))

    ;; free resources
    (cl-mysql:disconnect)))

(defun get-create-table (table-name cols)
  "Return a PostgreSQL CREATE TABLE statement from MySQL columns"
  (with-output-to-string (s)
    (format s "CREATE TABLE ~a ~%(~%" table-name)
    (loop
       for ((name dtype ctype default nullable extra) . last?) on cols
       for pg-coldef = (cast dtype ctype default nullable extra)
       do (format s "  ~a ~22t ~a~:[~;,~]~%" name pg-coldef last?))
    (format s ");~%")))

(defun get-drop-table-if-exists (table-name)
  "Return the PostgreSQL DROP TABLE IF EXISTS statement for TABLE-NAME."
  (format nil "DROP TABLE IF EXISTS ~a;~%" table-name))

(defun get-pgsql-create-tables (all-columns &key include-drop)
  "Return the list of CREATE TABLE statements to run against PostgreSQL"
  (loop
     for (table-name . cols) in all-columns
     when include-drop collect (get-drop-table-if-exists table-name)
     collect (get-create-table table-name cols)))

(defun pgsql-create-tables (dbname all-columns
			    &key (pg-dbname dbname) include-drop)
  "Create all MySQL tables in database dbname in PostgreSQL"
  (loop
     for nb-tables from 0
     for sql in (get-pgsql-create-tables all-columns :include-drop include-drop)
     do (pgloader.pgsql:execute pg-dbname sql)
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
		 "CREATE~:[~; UNIQUE~] INDEX ~a_idx ON ~a (~{~a~^, ~});"
		 unique index-name table-name cols))))

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

(defun pgsql-create-indexes (dbname &key (pg-dbname dbname) include-drop)
  "Create all MySQL tables in database dbname in PostgreSQL"
  (loop
     for nb-indexes from 0
     for sql in (get-pgsql-create-indexes (list-all-indexes dbname)
					  :include-drop include-drop)
     do (pgloader.pgsql:execute pg-dbname sql)
     finally (return nb-indexes)))

;;;
;;; Map a function to each row extracted from MySQL
;;;
(defun get-column-list-with-is-nulls (dbname table-name)
  "We can't just SELECT *, we need to cater for NULLs in text columns ourselves.
   This function assumes a valid connection to the MySQL server has been
   established already."
  (loop
     for (name type) in (caar (cl-mysql:query (format nil "
  select column_name, data_type
    from information_schema.columns
   where table_schema = '~a' and table_name = '~a'
order by ordinal_position" dbname table-name)))
     for is-null = (member type '("char" "varchar" "text"
			 "tinytext" "mediumtext" "longtext")
			   :test #'string-equal)
     collect nil into nulls
     collect name into cols
     when is-null
     collect (format nil "~a is null" name) into cols and collect t into nulls
     finally (return (values cols nulls))))

(defun fix-nulls (row nulls)
  "Given a cl-mysql row result and a nulls list as from
   get-column-list-with-is-nulls, replace NIL with empty strings with when
   we know from the added 'foo is null' that the actual value IS NOT NULL.

   See http://bugs.mysql.com/bug.php?id=19564 for context."
  (loop
     for (current-col  next-col)  on row
     for (current-null next-null) on nulls
     ;; next-null tells us if next column is an "is-null" col
     ;; when next-null is true, next-col is true if current-col is actually null
     for is-null = (and next-null (string= next-col "1"))
     for is-empty = (and next-null (string= next-col "0") (null current-col))
     ;; don't collect columns we added, e.g. "column_name is not null"
     when (not current-null)
     collect (cond (is-null :null)
		   (is-empty "")
		   (t current-col))))

(defun map-rows (dbname table-name &key process-row-fn)
  "Extract MySQL data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (cl-mysql:connect :host *myconn-host*
		    :port *myconn-port*
		    :user *myconn-user*
		    :password *myconn-pass*)

  (unwind-protect
       (progn
	 ;; Ensure we're talking utf-8 and connect to DBNAME in MySQL
	 (cl-mysql:query "SET NAMES 'utf8'")
	 (cl-mysql:query "SET character_set_results = utf8;")
	 (cl-mysql:use dbname)

	 (multiple-value-bind (cols nulls)
	     (get-column-list-with-is-nulls dbname table-name)
	  (let* ((sql  (format nil "SELECT ~{~a~^, ~} FROM ~a;" cols table-name))
		 (q    (cl-mysql:query sql :store nil :type-map nil))
		 (rs   (cl-mysql:next-result-set q)))
	    (declare (ignore rs))

	    ;; Now fetch MySQL rows directly in the stream
	    (loop
	       for row = (cl-mysql:next-row q :type-map (make-hash-table))
	       while row
	       for row-with-proper-nulls = (fix-nulls row nulls)
	       counting row into count
	       do (funcall process-row-fn row-with-proper-nulls)
	       finally (return count)))))

    ;; free resources
    (cl-mysql:disconnect)))

;;;
;;; Use map-rows and pgsql-text-copy-format to fill in a CSV file on disk
;;; with MySQL data in there.
;;;
(defun copy-to (dbname table-name filename &key transforms)
  "Extrat data from MySQL in PostgreSQL COPY TEXT format"
  (with-open-file (text-file filename
			     :direction :output
			     :if-exists :supersede
			     :external-format :utf-8)
    (map-rows dbname table-name
	      :process-row-fn
	      (lambda (row)
		(pgloader.pgsql:format-row text-file row
					   :transforms transforms)))))

;;;
;;; MySQL bulk export to file, in PostgreSQL COPY TEXT format
;;;
(defun export-database (dbname &key only-tables)
  "Export MySQL tables into as many TEXT files, in the PostgreSQL COPY format"
  (let ((all-columns  (list-all-columns dbname)))
    (setf *state* (pgloader.utils:make-pgstate))
    (report-header)
    (loop
       for (table-name . cols) in all-columns
       for filename = (pgloader.csv:get-pathname dbname table-name)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgstate-add-table *state* dbname table-name)
	 (report-table-name table-name)
	 (multiple-value-bind (rows secs)
	     (timing
	      ;; load data
	      (copy-to dbname table-name filename :transforms (transforms cols)))
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
				 (pg-dbname dbname)
				 (truncate t)
				 only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  ;; get the list of tables and have at it
  (let ((all-columns  (list-all-columns dbname)))
    (setf *state* (pgloader.utils:make-pgstate))
    (report-header)
    (loop
       for (table-name . cols) in all-columns
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgstate-add-table *state* dbname table-name)
	 (report-table-name table-name)

	 (multiple-value-bind (res secs)
	     (timing
	      (let* ((filename (pgloader.csv:get-pathname dbname table-name)))
		;; export from MySQL to file
		(copy-to dbname table-name filename :transforms (transforms cols))
		;; import the file to PostgreSQL
		(pgloader.pgsql:copy-from-file pg-dbname
					       table-name
					       filename
					       :truncate truncate)))
	   (declare (ignore res))
	   (pgstate-incf *state* table-name :secs secs)
	   (report-pgtable-stats *state* table-name))
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
		       (pg-dbname dbname)
		       truncate
		       transforms)
  "Connect in parallel to MySQL and PostgreSQL and stream the data."
  (let* ((lp:*kernel* *loader-kernel*)
	 (channel     (lp:make-channel))
	 (dataq       (lq:make-queue :fixed-capacity 4096)))
    (lp:submit-task channel (lambda ()
			      ;; this function update :read stats
			      (copy-to-queue dbname table-name dataq)))

    ;; and start another task to push that data from the queue to PostgreSQL
    (lp:submit-task
     channel
     (lambda ()
       ;; this function update :rows stats
       (pgloader.pgsql:copy-from-queue pg-dbname table-name dataq
				       :truncate truncate
				       :transforms transforms)))

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
       (pgstate-incf ,state ,table-name :rows res :secs secs)
       (report-pgtable-stats ,state ,table-name)

       ;; once those numbers are reporting, forget about them in the total
       (pgstate-decf ,state ,table-name :rows res))))

(defun stream-database (dbname
			&key
			  (pg-dbname dbname)
			  (create-tables nil)
			  (include-drop nil)
			  (create-indexes t)
			  (reset-sequences t)
			  (truncate t)
			  only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  ;; get the list of tables and have at it
  (let ((all-columns  (list-all-columns dbname)))
    (setf *state* (pgloader.utils:make-pgstate))
    (report-header)

    ;; if asked, first drop/create the tables on the PostgreSQL side
    (when create-tables
      (with-silent-timing *state* dbname
			  (format nil "~:[~;DROP then ~]CREATE TABLES" include-drop)
			  (pgsql-create-tables dbname all-columns
					       :pg-dbname pg-dbname
					       :include-drop include-drop)))

    (loop
       for (table-name . columns) in all-columns
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgstate-add-table *state* dbname table-name)
	 (report-table-name table-name)

	 (multiple-value-bind (res secs)
	     (timing
	      ;; this will care about updating stats in *state*
	      (stream-table dbname table-name
			    :pg-dbname pg-dbname
			    :truncate truncate
			    :transforms (transforms columns)))
	   ;; set the timing we just measured
	   (declare (ignore res))
	   (pgstate-incf *state* table-name :secs secs)
	   (report-pgtable-stats *state* table-name))
       finally
	 (when create-indexes
	   (with-silent-timing *state* dbname
	       (format nil "~:[~;DROP then ~]CREATE INDEXES" include-drop)
	     (pgsql-create-indexes dbname
				   :pg-dbname pg-dbname
				   :include-drop include-drop)))

       ;; don't forget to reset sequences
	 (when reset-sequences
	   (with-silent-timing *state* dbname "RESET SEQUENCES"
	     (pgloader.pgsql:reset-all-sequences pg-dbname)))

       ;; and report the total time spent on the operation
	 (report-pgstate-stats *state* "Total streaming time"))))


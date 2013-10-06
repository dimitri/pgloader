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

(defun list-views (dbname
		   &key
		     only-tables
		     (host *myconn-host*)
		     (user *myconn-user*)
		     (pass *myconn-pass*))
  "Return a flat list of all the view names and definitions known in given DBNAME"
  (cl-mysql:connect :host host :user user :password pass)

  (unwind-protect
       (progn
	 (cl-mysql:use dbname)
	 ;; that returns a pretty weird format, process it
	 (caar (cl-mysql:query (format nil "
  select table_name, view_definition
    from information_schema.views
   where table_schema = '~a'
         ~@[and table_name in (~{'~a'~^,~})~]
order by table_name" dbname only-tables))))
    ;; free resources
    (cl-mysql:disconnect)))

;;;
;;; Tools to get MySQL table and columns definitions and transform them to
;;; PostgreSQL CREATE TABLE statements, and run those.
;;;
(defun list-all-columns (dbname
			 &key
			   only-tables
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
  select c.table_name, c.column_name,
         c.data_type, c.column_type, c.column_default,
         c.is_nullable, c.extra
    from information_schema.columns c
         join information_schema.tables t using(table_schema, table_name)
   where c.table_schema = '~a' and t.table_type = 'BASE TABLE'
         ~@[and table_name in (~{'~a'~^,~})~]
order by table_name, ordinal_position" dbname only-tables)))
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

(defun get-create-type (table-name cols &key identifier-case include-drop)
  "MySQL declares ENUM types inline, PostgreSQL wants explicit CREATE TYPE."
  (loop
     for (name dtype ctype default nullable extra) in cols
     when (string-equal "enum" dtype)
     collect (when include-drop
	       (let* ((type-name
		       (get-enum-type-name table-name name identifier-case)))
		 (format nil "DROP TYPE IF EXISTS ~a;" type-name)))
     and collect (get-create-enum table-name name ctype
				  :identifier-case identifier-case)))

(defun get-create-table (table-name cols &key identifier-case)
  "Return a PostgreSQL CREATE TABLE statement from MySQL columns"
  (with-output-to-string (s)
    (let ((table-name (apply-identifier-case table-name identifier-case)))
      (format s "CREATE TABLE ~a ~%(~%" table-name))
    (loop
       for ((name dtype ctype default nullable extra) . last?) on cols
       for pg-coldef = (cast table-name name dtype ctype default nullable extra)
       for colname = (apply-identifier-case name identifier-case)
       do (format s "  ~a ~22t ~a~:[~;,~]~%" colname pg-coldef last?))
    (format s ");~%")))

(defun get-drop-table-if-exists (table-name &key identifier-case)
  "Return the PostgreSQL DROP TABLE IF EXISTS statement for TABLE-NAME."
  (let ((table-name (apply-identifier-case table-name identifier-case)))
    (format nil "DROP TABLE IF EXISTS ~a;~%" table-name)))

(defun get-pgsql-create-tables (all-columns
				&key
				  include-drop
				  (identifier-case :downcase))
  "Return the list of CREATE TABLE statements to run against PostgreSQL"
  (loop
     for (table-name . cols) in all-columns
     for extra-types = (get-create-type table-name cols
					:identifier-case identifier-case
					:include-drop include-drop)
     when include-drop
     collect (get-drop-table-if-exists table-name
				       :identifier-case identifier-case)

     when extra-types append extra-types

     collect (get-create-table table-name cols
			       :identifier-case identifier-case)))

(defun pgsql-create-tables (all-columns
			    &key
			      (identifier-case :downcase)
			      include-drop)
  "Create all MySQL tables in database dbname in PostgreSQL"
  (loop
     for nb-tables from 0
     for sql in (get-pgsql-create-tables all-columns
					 :identifier-case identifier-case
					 :include-drop include-drop)
     do (pgsql-execute sql :client-min-messages :warning)
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
	    with schema = nil
	    for (table-name . index-def)
	    in (caar (cl-mysql:query (format nil "
  SELECT table_name, index_name, non_unique,
         GROUP_CONCAT(column_name order by seq_in_index)
    FROM information_schema.statistics
   WHERE table_schema = '~a'
GROUP BY table_name, index_name;" dbname)))
	    do (let ((entry (assoc table-name schema :test 'equal))
		     (index-def
		      (destructuring-bind (index-name non-unique cols)
			  index-def
			(list index-name
			      (not (= 1 non-unique))
			      (sq:split-sequence #\, cols)))))
		 (if entry
		     (push index-def (cdr entry))
		     (push (cons table-name (list index-def)) schema)))
	    finally
	      ;; we did push, we need to reverse here
	      (return (reverse (loop
			  for (name . indexes) in schema
			  collect (cons name (reverse indexes)))))))

    ;; free resources
    (cl-mysql:disconnect)))

(defun get-pgsql-index-def (table-name table-oid index-name unique cols
			    &key identifier-case)
  "Return a PostgreSQL CREATE INDEX statement as a string."
  (let* ((index-name (format nil "idx_~a_~a" table-oid index-name))
	 (table-name (apply-identifier-case table-name identifier-case))
	 (index-name (apply-identifier-case index-name identifier-case))
	 (cols
	  (mapcar
	   (lambda (col) (apply-identifier-case col identifier-case)) cols)))
    (cond
      ((string= index-name "PRIMARY")
       (format nil
	       "ALTER TABLE ~a ADD PRIMARY KEY (~{~a~^, ~});" table-name cols))

      (t
       (format nil
	       "CREATE~:[~; UNIQUE~] INDEX ~a ON ~a (~{~a~^, ~});"
	       unique index-name table-name cols)))))

(defun get-drop-index-if-exists (table-name table-oid index-name
				 &key identifier-case)
  "Return the DROP INDEX statement for PostgreSQL"
  (cond
    ((string= index-name "PRIMARY")
     (let* ((pkey-name  (format nil "~a_pkey" table-name))
	    (pkey-name  (apply-identifier-case pkey-name identifier-case))
	    (table-name (apply-identifier-case table-name identifier-case)))
       (format nil "ALTER TABLE ~a DROP CONSTRAINT ~a;" table-name pkey-name)))

    (t
     (let* ((index-name (format nil "idx_~a_~a" table-oid index-name))
	    (index-name (apply-identifier-case index-name identifier-case)))
       (format nil "DROP INDEX IF EXISTS ~a;" index-name)))))

(defun get-pgsql-drop-indexes (table-name table-oid indexes &key identifier-case)
  "Return the DROP INDEX statements for given INDEXES definitions."
  (loop
     for (index-name unique cols) in indexes
     ;; no need to alter table drop constraint, when include-drop
     ;; is true we just did drop the table and created it again
     ;; anyway
     unless (string= index-name "PRIMARY")
     collect (get-drop-index-if-exists table-name table-oid index-name
				       :identifier-case identifier-case)))

(defun get-pgsql-create-indexes (table-name table-oid indexes
				 &key identifier-case)
  "Return the CREATE INDEX statements from given INDEXES definitions."
  (loop
     for (index-name unique cols) in indexes
     collect (get-pgsql-index-def table-name table-oid index-name unique cols
				  :identifier-case identifier-case)))


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
     collect (format nil "`~a`" name) into cols
     when is-null
     collect (format nil "`~a` is null" name) into cols and collect t into nulls
     finally (return (values cols nulls))))

(declaim (inline fix-nulls))

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
	       with type-map = (make-hash-table)
	       for row = (cl-mysql:next-row q :type-map type-map)
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
		       (kernel nil k-s-p)
		       (pg-dbname dbname)
		       truncate
		       transforms)
  "Connect in parallel to MySQL and PostgreSQL and stream the data."
  (let* ((summary     (null *state*))
	 (*state*     (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel* (or kernel (make-kernel 2)))
	 (channel     (lp:make-channel))
	 (dataq       (lq:make-queue :fixed-capacity 4096))
	 (transforms  (or transforms
			  (let* ((all-columns (list-all-columns dbname))
				 (our-columns
				  (cdr (assoc table-name all-columns
					      :test #'string=))))
			    (transforms our-columns)))))

    (with-stats-collection (dbname table-name :state *state* :summary summary)
      (log-message :notice "COPY ~a.~a" dbname table-name)
      ;; read data from MySQL
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
      (loop for tasks below 2 do (lp:receive-result channel)
	 finally
	   (log-message :info "COPY ~a.~a done." dbname table-name)
	   (unless k-s-p (lp:end-kernel))))))

;;;
;;; Work on all tables for given database
;;;
(defun execute-with-timing (dbname label sql state &key (count 1))
  "Execute given SQL and resgister its timing into STATE."
  (multiple-value-bind (res secs)
      (timing
       (with-pgsql-transaction (dbname)
	 (handler-case
	     (pgsql-execute sql)
	   (condition (e)
	     (log-message :error "~a" e)))))
    (declare (ignore res))
    (pgstate-incf state label :rows count :secs secs)))

(defun create-indexes-in-kernel (dbname table-name indexes kernel channel
				 &key
				   identifier-case include-drop
				   state (label "create index"))
  "Create indexes for given table in dbname, using given lparallel KERNEL
   and CHANNEL so that the index build happen in concurrently with the data
   copying."
  (let* ((lp:*kernel* kernel)
	 (table-oid
	  (with-pgsql-transaction (dbname)
	    (pomo:query
	     (format nil "select '~a'::regclass::oid" table-name) :single))))
    (pgstate-add-table state dbname label)

    (when include-drop
      (let ((drop-channel (lp:make-channel))
	    (drop-indexes
	     (get-pgsql-drop-indexes table-name table-oid indexes
				     :identifier-case identifier-case)))
	(loop
	   for sql in drop-indexes
	   do
	     (log-message :notice "~a" sql)
	     (lp:submit-task drop-channel
			     #'execute-with-timing dbname label sql state))

	;; wait for the DROP INDEX to be done before issuing CREATE INDEX
	(loop for idx in drop-indexes do (lp:receive-result drop-channel))))

    (loop
       for sql in (get-pgsql-create-indexes table-name table-oid indexes
					    :identifier-case identifier-case)
       do
	 (log-message :notice "~a" sql)
	 (lp:submit-task channel #'execute-with-timing dbname label sql state))))

(defun stream-database (dbname
			&key
			  (pg-dbname dbname)
			  (schema-only nil)
			  (create-tables nil)
			  (include-drop nil)
			  (create-indexes t)
			  (reset-sequences t)
			  (identifier-case :downcase) ; or :quote
			  (truncate t)
			  only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  ;; get the list of tables and have at it
  (let* ((*state*       (make-pgstate))
	 (idx-state     (make-pgstate))
         (copy-kernel   (make-kernel 2))
         (all-columns   (list-all-columns dbname :only-tables only-tables))
         (all-indexes   (list-all-indexes dbname))
         (max-indexes   (loop for (table . indexes) in all-indexes
                           maximizing (length indexes)))
         (idx-kernel    (when (and max-indexes (< 0 max-indexes))
			  (make-kernel max-indexes)))
         (idx-channel   (when idx-kernel
			  (let ((lp:*kernel* idx-kernel))
			    (lp:make-channel)))))

    ;; if asked, first drop/create the tables on the PostgreSQL side
    (when create-tables
      (log-message :notice "~:[~;DROP then ~]CREATE TABLES" include-drop)
      (with-pgsql-transaction (dbname)
	(pgsql-create-tables all-columns
			     :identifier-case identifier-case
			     :include-drop include-drop)))

    (loop
       for (table-name . columns) in all-columns
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (progn
	   ;; first COPY the data from MySQL to PostgreSQL, using copy-kernel
	   (unless schema-only
	     (stream-table dbname table-name
			   :kernel copy-kernel
			   :pg-dbname pg-dbname
			   :truncate truncate
			   :transforms (transforms columns)))

	   ;; Create the indexes for that table in parallel with the next
	   ;; COPY, and all at once in concurrent threads to benefit from
	   ;; PostgreSQL synchronous scan ability
	   ;;
	   ;; We just push new index build as they come along, if one
	   ;; index build requires much more time than the others our
	   ;; index build might get unsync: indexes for different tables
	   ;; will get built in parallel --- not a big problem.
	   (when create-indexes
	     (let* ((indexes
		     (cdr (assoc table-name all-indexes :test #'string=))))
	       (create-indexes-in-kernel dbname table-name indexes
					 idx-kernel idx-channel
					 :state idx-state
					 :include-drop include-drop
					 :identifier-case identifier-case)))))

    ;; don't forget to reset sequences, but only when we did actually import
    ;; the data.
    (when (and (not schema-only) reset-sequences)
      (let ((only-tables
	     (mapcar
	      (lambda (name) (apply-identifier-case name identifier-case))
	      only-tables)))
	(log-message :notice "Resetting all sequences")
	(pgloader.pgsql:reset-all-sequences pg-dbname :only-tables only-tables)))

    ;; now end the kernels
    (let ((lp:*kernel* idx-kernel))  (lp:end-kernel))
    (let ((lp:*kernel* copy-kernel))
      ;; wait until the indexes are done being built...
      ;; don't forget accounting for that waiting time.
      (with-stats-collection (dbname "index build completion" :state *state*)
	(loop for idx in all-indexes do (lp:receive-result idx-channel)))
      (lp:end-kernel))

    ;; and report the total time spent on the operation
    (report-summary)
    (format t pgloader.utils::*header-line*)
    (report-summary :state idx-state :header nil :footer nil)
    (report-pgstate-stats *state* "Total streaming time")))


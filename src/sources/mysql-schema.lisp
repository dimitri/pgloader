;;;
;;; Tools to query the MySQL Schema to reproduce in PostgreSQL
;;;

(in-package pgloader.mysql)


;;;
;;; Specific implementation of schema migration, see the API in
;;; src/pgsql/schema.lisp
;;;
(defstruct (mysql-column
	     (:constructor make-mysql-column
			   (table-name name dtype ctype default nullable extra)))
  table-name name dtype ctype default nullable extra)

(defmethod format-pgsql-column ((col mysql-column) &key identifier-case)
  "Return a string representing the PostgreSQL column definition."
  (let* ((column-name
	  (apply-identifier-case (mysql-column-name col) identifier-case))
	 (type-definition
	  (with-slots (table-name name dtype ctype default nullable extra)
	      col
	    (cast table-name name dtype ctype default nullable extra))))
    (format nil "~a ~22t ~a" column-name type-definition)))

(defmethod format-extra-type ((col mysql-column)
			      &key identifier-case include-drop)
  "Return a string representing the extra needed PostgreSQL CREATE TYPE
   statement, if such is needed"
  (let ((dtype (mysql-column-dtype col)))
    (when (or (string-equal "enum" dtype)
	      (string-equal "set" dtype))
      (list
       (when include-drop
	 (let* ((type-name
		 (get-enum-type-name (mysql-column-table-name col)
				     (mysql-column-name col)
				     identifier-case)))
		 (format nil "DROP TYPE IF EXISTS ~a;" type-name)))

       (get-create-enum (mysql-column-table-name col)
			(mysql-column-name col)
			(mysql-column-ctype col)
			:identifier-case identifier-case)))))


;;;
;;; Function for accessing the MySQL catalogs, implementing auto-discovery
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

(defun create-my-views (dbname views-alist
			&key
			  (host *myconn-host*)
			  (user *myconn-user*)
			  (pass *myconn-pass*))
  "VIEWS-ALIST associates view names with their SQL definition, which might
   be empty for already existing views. Create only the views for which we
   have an SQL definition."
  (let ((views (remove-if #'null views-alist :key #'cdr)))
    (when views
      (cl-mysql:connect :host host :user user :password pass)
      (unwind-protect
	   (progn
	     (cl-mysql:use dbname)
	     (loop for (name . def) in views
		for sql = (format nil "CREATE VIEW ~a AS ~a" name def)
		do
		  (log-message :info "MySQL: ~a" sql)
		  (cl-mysql:query sql)))
	;; free resources
	(cl-mysql:disconnect)))))

(defun drop-my-views (dbname views-alist
		      &key
			(host *myconn-host*)
			(user *myconn-user*)
			(pass *myconn-pass*))
  "See `create-my-views' for VIEWS-ALIST description. This time we DROP the
   views to clean out after our work."
  (let ((views (remove-if #'null views-alist :key #'cdr)))
    (when views
      (cl-mysql:connect :host host :user user :password pass)
      (unwind-protect
	   (let ((sql
		  (format nil "DROP VIEW ~{~a~^, ~};" (mapcar #'car views))))
	     (cl-mysql:use dbname)
	     (log-message :info "MySQL: ~a" sql)
	     (cl-mysql:query sql))
	;; free resources
	(cl-mysql:disconnect)))))


;;;
;;; Tools to get MySQL table and columns definitions and transform them to
;;; PostgreSQL CREATE TABLE statements, and run those.
;;;
(defvar *table-type* '((:table . "BASE TABLE")
		       (:view  . "VIEW"))
  "Associate internal table type symbol with what's found in MySQL
  information_schema.tables.table_type column.")

(defun list-all-columns (dbname
			 &key
			   only-tables
			   (host *myconn-host*)
			   (user *myconn-user*)
			   (pass *myconn-pass*)
			   (table-type :table)
			 &aux
			   (table-type-name (cdr (assoc table-type *table-type*))))
  "Get the list of MySQL column names per table."
  (cl-mysql:connect :host host :user user :password pass)

  (unwind-protect
       (progn
	 (loop
	    with schema = nil
	    for (table-name name dtype ctype default nullable extra)
	    in
	      (caar (cl-mysql:query (format nil "
  select c.table_name, c.column_name,
         c.data_type, c.column_type, c.column_default,
         c.is_nullable, c.extra
    from information_schema.columns c
         join information_schema.tables t using(table_schema, table_name)
   where c.table_schema = '~a' and t.table_type = '~a'
         ~@[and table_name in (~{'~a'~^,~})~]
order by table_name, ordinal_position" dbname table-type-name only-tables)))
	    do
	      (let ((entry  (assoc table-name schema :test 'equal))
		    (column
		     (make-mysql-column
		      table-name name dtype ctype default nullable extra)))
		(if entry
		    (push column (cdr entry))
		    (push (cons table-name (list column)) schema)))
	    finally
	      ;; we did push, we need to reverse here
	      (return (loop
			 for (name . cols) in schema
			 collect (cons name (reverse cols))))))

    ;; free resources
    (cl-mysql:disconnect)))

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
	    for (table-name name non-unique cols)
	    in (caar (cl-mysql:query (format nil "
  SELECT table_name, index_name, non_unique,
         GROUP_CONCAT(column_name order by seq_in_index)
    FROM information_schema.statistics
   WHERE table_schema = '~a'
GROUP BY table_name, index_name;" dbname)))
	    do (let ((entry (assoc table-name schema :test 'equal))
		     (index
		      (make-pgsql-index :name name
					:primary (string= name "PRIMARY")
					:table-name table-name
					:unique (not (= 1 non-unique))
					:columns (sq:split-sequence #\, cols))))
		 (if entry
		     (push index (cdr entry))
		     (push (cons table-name (list index)) schema)))
	    finally
	      ;; we did push, we need to reverse here
	      (return (reverse (loop
			  for (name . indexes) in schema
			  collect (cons name (reverse indexes)))))))

    ;; free resources
    (cl-mysql:disconnect)))

(defun set-table-oids (all-indexes)
  "MySQL allows using the same index name against separate tables, which
   PostgreSQL forbids. To get unicity in index names without running out of
   characters (we are allowed only 63), we use the table OID instead.

   This function grabs the table OIDs in the PostgreSQL database and update
   the definitions with them."
  (let* ((table-names (mapcar #'car all-indexes))
	 (table-oids  (pgloader.pgsql:list-table-oids table-names)))
    (loop for (table-name . indexes) in all-indexes
       for table-oid = (cdr (assoc table-name table-oids :test #'string=))
       unless table-oid do (error "OID not found for ~s." table-name)
       do (loop for index in indexes
	     do (setf (pgloader.pgsql::pgsql-index-table-oid index) table-oid)))))


;;;
;;; MySQL Foreign Keys
;;;
(defun list-all-fkeys (dbname
			 &key
			   (host *myconn-host*)
			   (user *myconn-user*)
			   (pass *myconn-pass*))
  "Get the list of MySQL Foreign Keys definitions per table."
  (cl-mysql:connect :host host :user user :password pass)

  (unwind-protect
       (progn
	 (loop
	    with schema = nil
	    for (table-name name ftable cols fcols)
	    in (caar (cl-mysql:query (format nil "
    SELECT i.table_name, i.constraint_name, k.referenced_table_name ft,

           group_concat(         k.column_name
                        order by k.ordinal_position) as cols,

           group_concat(         k.referenced_column_name
                        order by k.position_in_unique_constraint) as fcols

      FROM information_schema.table_constraints i
      LEFT JOIN information_schema.key_column_usage k
          USING (table_schema, table_name, constraint_name)

    WHERE     i.table_schema = '~a'
          AND k.referenced_table_schema = '~a'
          AND i.constraint_type = 'FOREIGN KEY'

 GROUP BY table_name, constraint_name, ft;" dbname dbname)))
	    do (let ((entry (assoc table-name schema :test 'equal))
		     (fk    (make-pgsql-fkey :name name
					     :table-name table-name
					     :columns cols
					     :foreign-table ftable
					     :foreign-columns fcols)))
		 (if entry
		     (push fk (cdr entry))
		     (push (cons table-name (list fk)) schema)))
	    finally
	      ;; we did push, we need to reverse here
	      (return (reverse (loop
			  for (name . fks) in schema
			  collect (cons name (reverse fks)))))))

    ;; free resources
    (cl-mysql:disconnect)))

(defun drop-fkeys (all-fkeys &key dbname identifier-case)
  "Drop all Foreign Key Definitions given, to prepare for a clean run."
  (let ((all-pgsql-fkeys (list-tables-and-fkeys dbname)))
    (loop for (table-name . fkeys) in all-fkeys
       do
	 (loop for fkey in fkeys
	    for sql = (format-pgsql-drop-fkey fkey
					      :all-pgsql-fkeys all-pgsql-fkeys
					      :identifier-case identifier-case)
	    when sql
	    do
	      (log-message :notice "~a;" sql)
	      (pgsql-execute sql)))))

(defun create-fkeys (all-fkeys
		     &key
		       dbname state identifier-case (label "Foreign Keys"))
  "Actually create the Foreign Key References that where declared in the
   MySQL database"
  (pgstate-add-table state dbname label)
  (loop for (table-name . fkeys) in all-fkeys
     do (loop for fkey in fkeys
	   for sql =
	     (format-pgsql-create-fkey fkey :identifier-case identifier-case)
	   do
	     (log-message :notice "~a;" sql)
	     (pgsql-execute-with-timing dbname "Foreign Keys" sql state))))


;;;
;;; Sequences
;;;
(defun reset-sequences (all-columns &key dbname state identifier-case)
  "Reset all sequences created during this MySQL migration."
  (let ((tables
	 (mapcar
	  (lambda (name) (apply-identifier-case name identifier-case))
	  (mapcar #'car all-columns))))
    (log-message :notice "Reset sequences")
    (with-stats-collection (dbname "Reset Sequences"
				   :use-result-as-rows t
				   :state state)
      (pgloader.pgsql:reset-all-sequences dbname :tables tables))))


;;;
;;; Tools to handle row queries, issuing separate is null statements and
;;; handling of geometric data types.
;;;
(defun get-column-sql-expression (name type)
  "Return per-TYPE SQL expression to use given a column NAME.

   Mostly we just use the name, but in case of POINT we need to use
   astext(name)."
  (case (intern (string-upcase type) "KEYWORD")
    (:point  (format nil "astext(`~a`) as `~a`" name name))
    (t       (format nil "`~a`" name))))

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
     collect (get-column-sql-expression name type) into cols
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


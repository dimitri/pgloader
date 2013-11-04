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
	    for (table-name name dtype ctype default nullable extra)
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


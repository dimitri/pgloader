;;;
;;; Tools to query the MySQL Schema to reproduce in PostgreSQL
;;;

(in-package :pgloader.mysql)

(defvar *connection* nil "Current MySQL connection")


;;;
;;; Specific implementation of schema migration, see the API in
;;; src/pgsql/schema.lisp
;;;
(defstruct (mysql-column
	     (:constructor make-mysql-column
			   (table-name name dtype ctype default nullable extra)))
  table-name name dtype ctype default nullable extra)

(defmethod format-pgsql-column ((col mysql-column))
  "Return a string representing the PostgreSQL column definition."
  (let* ((column-name (apply-identifier-case (mysql-column-name col)))
	 (type-definition
	  (with-slots (table-name name dtype ctype default nullable extra)
	      col
	    (cast table-name name dtype ctype default nullable extra))))
    (format nil "~a ~22t ~a" column-name type-definition)))

(defmethod format-extra-type ((col mysql-column) &key include-drop)
  "Return a string representing the extra needed PostgreSQL CREATE TYPE
   statement, if such is needed"
  (let ((dtype (mysql-column-dtype col)))
    (when (or (string-equal "enum" dtype)
	      (string-equal "set" dtype))
      (list
       (when include-drop
	 (let* ((type-name
		 (get-enum-type-name (mysql-column-table-name col)
				     (mysql-column-name col))))
		 (format nil "DROP TYPE IF EXISTS ~a;" type-name)))

       (get-create-enum (mysql-column-table-name col)
			(mysql-column-name col)
			(mysql-column-ctype col))))))


;;;
;;; General utility to manage MySQL connection
;;;
(defun mysql-query (query &key row-fn (as-text t) (result-type 'list))
  "Execute given QUERY within the current *connection*, and set proper
   defaults for pgloader."
  (qmynd:mysql-query *connection* query
                     :row-fn row-fn
                     :as-text as-text
                     :result-type result-type))

(defmacro with-mysql-connection ((&optional (dbname *my-dbname*)) &body forms)
  "Connect to MySQL, use given DBNAME as the current database if provided,
   and execute FORMS in a protected way so that we always disconnect when
   done.

   Connection parameters are *myconn-host*, *myconn-port*, *myconn-user* and
   *myconn-pass*."
  `(let* ((dbname (or ,dbname *my-dbname*))
          (*connection*
           (if (and (consp *myconn-host*) (eq :unix (car *myconn-host*)))
               (qmynd:mysql-local-connect :path (cdr *myconn-host*)
                                          :username *myconn-user*
                                          :password *myconn-pass*
                                          :database dbname)
               (qmynd:mysql-connect :host *myconn-host*
                                    :port *myconn-port*
                                    :username *myconn-user*
                                    :password *myconn-pass*
                                    :database dbname))))
     (unwind-protect
          (progn ,@forms)
       (qmynd:mysql-disconnect *connection*))))

;;;
;;; Function for accessing the MySQL catalogs, implementing auto-discovery.
;;;
;;; Interactive use only, will create its own database connection.
;;;
(defun list-databases ()
  "Connect to a local database and get the database list"
  (with-mysql-connection ()
    (mysql-query "show databases")))

(defun list-tables (dbname)
  "Return a flat list of all the tables names known in given DATABASE"
  (with-mysql-connection (dbname)
    (mysql-query (format nil "
  select table_name
    from information_schema.tables
   where table_schema = '~a' and table_type = 'BASE TABLE'
order by table_name" dbname))))

(defun list-views (dbname &key only-tables)
  "Return a flat list of all the view names and definitions known in given DBNAME"
  (with-mysql-connection (dbname)
    (mysql-query (format nil "
  select table_name, view_definition
    from information_schema.views
   where table_schema = '~a'
         ~@[and table_name in (~{'~a'~^,~})~]
order by table_name" dbname only-tables))))


;;;
;;; Those functions are to be called from withing an already established
;;; MySQL Connection.
;;;
;;; Handle MATERIALIZE VIEWS sections, where we need to create the views in
;;; the MySQL database before being able to process them.
;;;
(defun create-my-views (views-alist)
  "VIEWS-ALIST associates view names with their SQL definition, which might
   be empty for already existing views. Create only the views for which we
   have an SQL definition."
  (unless (eq :all views-alist)
   (let ((views (remove-if #'null views-alist :key #'cdr)))
     (when views
       (loop for (name . def) in views
          for sql = (format nil "CREATE VIEW ~a AS ~a" name def)
          do
            (log-message :info "MySQL: ~a" sql)
            (mysql-query sql))))))

(defun drop-my-views (views-alist)
  "See `create-my-views' for VIEWS-ALIST description. This time we DROP the
   views to clean out after our work."
  (unless (eq :all views-alist)
   (let ((views (remove-if #'null views-alist :key #'cdr)))
     (when views
       (let ((sql
              (format nil "DROP VIEW ~{~a~^, ~};" (mapcar #'car views))))
         (log-message :info "MySQL: ~a" sql)
         (mysql-query sql))))))


;;;
;;; Those functions are to be called from withing an already established
;;; MySQL Connection.
;;;
;;; Tools to get MySQL table and columns definitions and transform them to
;;; PostgreSQL CREATE TABLE statements, and run those.
;;;
(defvar *table-type* '((:table . "BASE TABLE")
		       (:view  . "VIEW"))
  "Associate internal table type symbol with what's found in MySQL
  information_schema.tables.table_type column.")

(defun filter-list-to-where-clause (filter-list &optional not)
  "Given an INCLUDING or EXCLUDING clause, turn it into a MySQL WHERE clause."
  (mapcar (lambda (filter)
            (typecase filter
              (string (format nil "~:[~;!~]= '~a'" not filter))
              (cons   (format nil "~:[~;NOT ~]REGEXP '~a'" not (cadr filter)))))
          filter-list))

(defun cleanup-default-value (dtype default)
  "MySQL catalog query always returns the default value as a string, but in
   the case of a binary data type we actually want a byte vector."
  (cond ((string= "binary" dtype)
         (when default
           (babel:string-to-octets default)))

        (t default)))

(defun list-all-columns (&key
                           (dbname *my-dbname*)
			   (table-type :table)
			   only-tables
                           including
                           excluding
			 &aux
			   (table-type-name (cdr (assoc table-type *table-type*))))
  "Get the list of MySQL column names per table."
  (loop
     with schema = nil
     for (table-name name dtype ctype default nullable extra)
     in
       (mysql-query (format nil "
  select c.table_name, c.column_name,
         c.data_type, c.column_type, c.column_default,
         c.is_nullable, c.extra
    from information_schema.columns c
         join information_schema.tables t using(table_schema, table_name)
   where c.table_schema = '~a' and t.table_type = '~a'
         ~:[~*~;and table_name in (~{'~a'~^,~})~]
         ~:[~*~;and (~{table_name ~a~^ or ~})~]
         ~:[~*~;and (~{table_name ~a~^ and ~})~]
order by table_name, ordinal_position"
                            dbname
                            table-type-name
                            only-tables ; do we print the clause?
                            only-tables
                            including   ; do we print the clause?
                            (filter-list-to-where-clause including)
                            excluding   ; do we print the clause?
                            (filter-list-to-where-clause excluding t)))
     do
       (let* ((entry   (assoc table-name schema :test 'equal))
              (def-val (cleanup-default-value dtype default))
              (column  (make-mysql-column
                        table-name name dtype ctype def-val nullable extra)))
         (if entry
             (push column (cdr entry))
             (push (cons table-name (list column)) schema)))
     finally
     ;; we did push, we need to reverse here
       (return (loop
                  for name in (if only-tables only-tables
                                  (reverse (mapcar #'car schema)))
                  for cols = (cdr (assoc name schema :test #'string=))
                  collect (cons name (reverse cols))))))

(defun list-all-indexes (&key
                           (dbname *my-dbname*)
                           only-tables
                           including
                           excluding)
  "Get the list of MySQL index definitions per table."
  (loop
     with schema = nil
     for (table-name name non-unique cols)
     in (mysql-query (format nil "
  SELECT table_name, index_name, non_unique,
         cast(GROUP_CONCAT(column_name order by seq_in_index) as char)
    FROM information_schema.statistics
   WHERE table_schema = '~a'
         ~:[~*~;and table_name in (~{'~a'~^,~})~]
         ~:[~*~;and (~{table_name ~a~^ or ~})~]
         ~:[~*~;and (~{table_name ~a~^ and ~})~]
GROUP BY table_name, index_name;"
                             dbname
                             only-tables ; do we print the clause?
                             only-tables
                             including   ; do we print the clause?
                             (filter-list-to-where-clause including)
                             excluding   ; do we print the clause?
                             (filter-list-to-where-clause excluding t)))
     do (let ((entry (assoc table-name schema :test 'equal))
              (index
               (make-pgsql-index :name name
                                 :primary (string= name "PRIMARY")
                                 :table-name table-name
                                 :unique (not (string= "1" non-unique))
                                 :columns (sq:split-sequence #\, cols))))
          (if entry
              (push index (cdr entry))
              (push (cons table-name (list index)) schema)))
     finally
     ;; we did push, we need to reverse here
       (return (reverse (loop
                           for (name . indexes) in schema
                           collect (cons name (reverse indexes)))))))

(defun set-table-oids (all-indexes)
  "MySQL allows using the same index name against separate tables, which
   PostgreSQL forbids. To get unicity in index names without running out of
   characters (we are allowed only 63), we use the table OID instead.

   This function grabs the table OIDs in the PostgreSQL database and update
   the definitions with them."
  (let* ((table-names (mapcar #'apply-identifier-case
                              (mapcar #'car all-indexes)))
	 (table-oids  (pgloader.pgsql:list-table-oids table-names)))
    (loop for (table-name-raw . indexes) in all-indexes
       for table-name = (apply-identifier-case table-name-raw)
       for table-oid = (cdr (assoc table-name table-oids :test #'string=))
       unless table-oid do (error "OID not found for ~s." table-name)
       do (loop for index in indexes
	     do (setf (pgloader.pgsql::pgsql-index-table-oid index) table-oid)))))

;;;
;;; MySQL Foreign Keys
;;;
(defun list-all-fkeys (&key
                         (dbname *my-dbname*)
                         only-tables
                         including
                         excluding)
  "Get the list of MySQL Foreign Keys definitions per table."
  (loop
     with schema = nil
     for (table-name name ftable cols fcols)
     in (mysql-query (format nil "
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
         ~:[~*~;and table_name in (~{'~a'~^,~})~]
         ~:[~*~;and (~{table_name ~a~^ or ~})~]
         ~:[~*~;and (~{table_name ~a~^ and ~})~]

 GROUP BY table_name, constraint_name, ft;"
                             dbname dbname
                             only-tables ; do we print the clause?
                             only-tables
                             including   ; do we print the clause?
                             (filter-list-to-where-clause including)
                             excluding   ; do we print the clause?
                             (filter-list-to-where-clause excluding t)))
     do (let ((entry (assoc table-name schema :test 'equal))
              (fk
               (make-pgsql-fkey :name name
                                :table-name table-name
                                :columns (sq:split-sequence #\, cols)
                                :foreign-table ftable
                                :foreign-columns (sq:split-sequence #\, fcols))))
          (if entry
              (push fk (cdr entry))
              (push (cons table-name (list fk)) schema)))
     finally
     ;; we did push, we need to reverse here
       (return (reverse (loop
                           for (name . fks) in schema
                           collect (cons name (reverse fks)))))))


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

(defun get-column-list (dbname table-name)
  "Some MySQL datatypes have a meaningless default output representation, we
   need to process them on the SQL side (geometric data types).

   This function assumes a valid connection to the MySQL server has been
   established already."
  (loop
     for (name type) in (mysql-query (format nil "
  select column_name, data_type
    from information_schema.columns
   where table_schema = '~a' and table_name = '~a'
order by ordinal_position" dbname table-name)
					   :result-type 'list)
     collect (get-column-sql-expression name type)))

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


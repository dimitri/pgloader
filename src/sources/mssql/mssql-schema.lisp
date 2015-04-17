;;;
;;; Tools to query the MS SQL Schema to reproduce in PostgreSQL
;;;

(in-package :pgloader.mssql)

(defvar *mssql-db* nil
  "The MS SQL database connection handler.")

;;;
;;; General utility to manage MySQL connection
;;;
(defclass mssql-connection (db-connection) ())

(defmethod initialize-instance :after ((msconn mssql-connection) &key)
  "Assign the type slot to mssql."
  (setf (slot-value msconn 'type) "mssql"))

(defmethod open-connection ((msconn mssql-connection) &key)
  (setf (conn-handle msconn) (mssql:connect (db-name msconn)
                                            (db-user msconn)
                                            (db-pass msconn)
                                            (db-host msconn)))
  ;; return the connection object
  msconn)

(defmethod close-connection ((msconn mssql-connection))
  (mssql:disconnect (conn-handle msconn))
  (setf (conn-handle msconn) nil)
  msconn)

(defun mssql-query (query)
  "Execute given QUERY within the current *connection*, and set proper
   defaults for pgloader."
  (mssql:query query :connection (conn-handle *mssql-db*)))


;;;
;;; We store the whole database schema in memory with the following
;;; organisation:
;;;
;;;  - an alist of (schema . tables)
;;;     - where tables is an alist of (name . cols)
;;;        - where cols is a list of mssql-column struct instances
;;;
(defun qualify-name (schema table-name)
  "Return the fully qualified name."
  (let ((sn (apply-identifier-case schema))
        (tn (apply-identifier-case table-name)))
    (format nil "~a.~a" sn tn)))

(defun qualified-table-name-list (schema-table-cols-alist)
  "Return a flat list of qualified table names."
  (loop :for (schema . tables) :in schema-table-cols-alist
     :append (loop :for (table . cols) :in tables
                :collect (cons (qualify-name schema table) cols))))


;;;
;;; Those functions are to be called from withing an already established
;;; MS SQL Connection.
;;;
;;; Tools to get MS SQL table and columns definitions and transform them to
;;; PostgreSQL CREATE TABLE statements, and run those.
;;;

(defvar *table-type* '((:table . "BASE TABLE")
		       (:view  . "VIEW"))
  "Associate internal table type symbol with what's found in MS SQL
  information_schema.tables.table_type column.")

(defun filter-list-to-where-clause (filter-list
                                    &optional
                                      not
                                      (schema-col "table_schema")
                                      (table-col  "table_name"))
  "Given an INCLUDING or EXCLUDING clause, turn it into a MS SQL WHERE clause."
  (loop :for (schema . table-name-list) :in filter-list
     :append (mapcar (lambda (table-name)
                       (format nil "(~a = '~a' and ~a ~:[~;NOT ~]LIKE '~a')"
                               schema-col schema table-col not table-name))
                     table-name-list)))

(defun list-all-columns (&key
			   (table-type :table)
                           including
                           excluding
			 &aux
			   (table-type-name (cdr (assoc table-type *table-type*))))
  (loop
     :with result := nil
     :for (schema table-name name type default nullable identity
                  character-maximum-length
                  numeric-precision numeric-precision-radix numeric-scale
                  datetime-precision
                  character-set-name collation-name)
     :in
     (mssql-query (format nil "
  select c.table_schema,
         c.table_name,
         c.column_name,
         c.data_type,
         CASE
             WHEN c.column_default LIKE '((%' AND c.column_default LIKE '%))' THEN SUBSTRING(c.column_default,3,len(c.column_default)-4)
             WHEN c.column_default LIKE '(%' AND c.column_default LIKE '%)' THEN SUBSTRING(c.column_default,2,len(c.column_default)-2)
         ELSE c.column_default END,
         c.is_nullable,
         COLUMNPROPERTY(object_id(c.table_name), c.column_name, 'IsIdentity'),
         c.CHARACTER_MAXIMUM_LENGTH,
         c.NUMERIC_PRECISION,
         c.NUMERIC_PRECISION_RADIX,
         c.NUMERIC_SCALE,
         c.DATETIME_PRECISION,
         c.CHARACTER_SET_NAME,
         c.COLLATION_NAME

    from information_schema.columns c
         join information_schema.tables t
              on c.table_schema = t.table_schema
             and c.table_name = t.table_name

   where     c.table_catalog = '~a'
         and t.table_type = '~a'
         ~:[~*~;and (~{~a~^~&~10t or ~})~]
         ~:[~*~;and (~{~a~^~&~10t and ~})~]

order by c.table_schema, c.table_name, c.ordinal_position"
                          (db-name *mssql-db*)
                          table-type-name
                          including   ; do we print the clause?
                          (filter-list-to-where-clause including
                                                       nil
                                                       "c.table_schema"
                                                       "c.table_name")
                          excluding   ; do we print the clause?
                          (filter-list-to-where-clause excluding
                                                       t
                                                       "c.table_schema"
                                                       "c.table_name")))
     :do
     (let* ((s-entry    (assoc schema result :test 'equal))
            (t-entry    (when s-entry
                          (assoc table-name (cdr s-entry) :test 'equal)))
            (column
             (make-mssql-column
              schema table-name name type default nullable
              (eq 1 identity)
              character-maximum-length
              numeric-precision numeric-precision-radix numeric-scale
              datetime-precision
              character-set-name collation-name)))
       (if s-entry
           (if t-entry
               (push column (cdr t-entry))
               (push (cons table-name (list column)) (cdr s-entry)))
           (push (cons schema (list (cons table-name (list column)))) result)))
     :finally
     ;; we did push, we need to reverse here
     (return (reverse
              (loop :for (schema . tables) :in result
                 :collect
                 (cons schema
                       (reverse (loop :for (table-name . cols) :in tables
                                   :collect (cons table-name (reverse cols))))))))))

(defun list-all-indexes (&key including excluding)
  "Get the list of MSSQL index definitions per table."
  (loop
     :with result := nil
     :for (schema table name col unique pkey)
     :in  (mssql-query (format nil "
    select schema_name(schema_id) as SchemaName,
           o.name as TableName,
           REPLACE(i.name, '.', '_') as IndexName,
           co.[name] as ColumnName,
           i.is_unique,
           i.is_primary_key

    from sys.indexes i
         join sys.objects o on i.object_id = o.object_id
         join sys.index_columns ic on ic.object_id = i.object_id
             and ic.index_id = i.index_id
         join sys.columns co on co.object_id = i.object_id
             and co.column_id = ic.column_id

   where schema_name(schema_id) not in ('dto', 'sys')
         ~:[~*~;and (~{~a~^ or ~})~]
         ~:[~*~;and (~{~a~^ and ~})~]

order by SchemaName,
         o.[name],
         i.[name],
         ic.is_included_column,
         ic.key_ordinal"
                               including ; do we print the clause?
                               (filter-list-to-where-clause including
                                                            nil
                                                            "schema_name(schema_id)"
                                                            "o.name"
                                                            )
                               excluding ; do we print the clause?
                               (filter-list-to-where-clause excluding
                                                            t
                                                            "schema_name(schema_id)"
                                                            "o.name"
                                                            )))
     :do
     (let* ((s-entry    (assoc schema result :test 'equal))
            (t-entry    (when s-entry
                          (assoc table (cdr s-entry) :test 'equal)))
            (i-entry    (when t-entry
                          (assoc name (cdr t-entry) :test 'equal)))
            (index      (make-pgsql-index :name name
                                          :primary (= pkey 1)
                                          :table-name (qualify-name schema table)
                                          :unique (= unique 1)
                                          :columns (list col))))
       (if s-entry
           (if t-entry
               (if i-entry
                   (push col
                         (pgloader.pgsql::pgsql-index-columns (cdr i-entry)))
                   (push (cons name index) (cdr t-entry)))
               (push (cons table (list (cons name index))) (cdr s-entry)))
           (push (cons schema
                       (list (cons table
                                   (list (cons name index))))) result)))
     :finally
     ;; we did push, we need to reverse here
     (return
       (labels ((reverse-index-cols (index)
                  (setf (pgloader.pgsql::pgsql-index-columns index)
                        (nreverse (pgloader.pgsql::pgsql-index-columns index)))
                  index)

                (reverse-indexes-cols (list-of-indexes)
                  (loop :for (name . index) :in list-of-indexes
                     :collect (cons name (reverse-index-cols index))))

                (reverse-indexes-cols (list-of-tables)
                  (reverse
                   (loop :for (table . indexes) :in list-of-tables
                      :collect (cons table (reverse-indexes-cols indexes))))))
         (reverse
          (loop :for (schema . tables) :in result
             :collect (cons schema (reverse-indexes-cols tables))))))))

(defun list-all-fkeys (&key including excluding)
  "Get the list of MSSQL index definitions per table."
  (loop
     :with result := nil
     :for (name schema table col fschema ftable fcol)
     :in  (mssql-query (format nil "
   SELECT
           REPLACE(KCU1.CONSTRAINT_NAME, '.', '_') AS 'CONSTRAINT_NAME'
         , KCU1.TABLE_SCHEMA AS 'TABLE_SCHEMA'
         , KCU1.TABLE_NAME AS 'TABLE_NAME'
         , KCU1.COLUMN_NAME AS 'COLUMN_NAME'
         , KCU2.TABLE_SCHEMA AS 'UNIQUE_TABLE_SCHEMA'
         , KCU2.TABLE_NAME AS 'UNIQUE_TABLE_NAME'
         , KCU2.COLUMN_NAME AS 'UNIQUE_COLUMN_NAME'

    FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC
         JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU1
              ON KCU1.CONSTRAINT_CATALOG = RC.CONSTRAINT_CATALOG
                 AND KCU1.CONSTRAINT_SCHEMA = RC.CONSTRAINT_SCHEMA
                 AND KCU1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
         JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU2
              ON KCU2.CONSTRAINT_CATALOG = RC.UNIQUE_CONSTRAINT_CATALOG
                 AND KCU2.CONSTRAINT_SCHEMA = RC.UNIQUE_CONSTRAINT_SCHEMA
                 AND KCU2.CONSTRAINT_NAME = RC.UNIQUE_CONSTRAINT_NAME

   WHERE KCU1.ORDINAL_POSITION = KCU2.ORDINAL_POSITION
         AND KCU1.TABLE_CATALOG = '~a'
         AND KCU1.CONSTRAINT_CATALOG = '~a'
         AND KCU1.CONSTRAINT_SCHEMA NOT IN ('dto', 'sys')
         AND KCU1.TABLE_SCHEMA NOT IN ('dto', 'sys')
         AND KCU2.TABLE_SCHEMA NOT IN ('dto', 'sys')

         ~:[~*~;and (~{~a~^ or ~})~]
         ~:[~*~;and (~{~a~^ and ~})~]

ORDER BY KCU1.CONSTRAINT_NAME, KCU1.ORDINAL_POSITION"
                               (db-name *mssql-db*) (db-name *mssql-db*)
                               including ; do we print the clause?
                               (filter-list-to-where-clause including
                                                            nil
                                                            "kcu1.table_schema"
                                                            "kcu1.table_name")
                               excluding ; do we print the clause?
                               (filter-list-to-where-clause excluding
                                                            t
                                                            "kcu1.table_schema"
                                                            "kcu1.table_name")))
     :do
     (let* ((s-entry    (assoc schema result :test 'equal))
            (t-entry    (when s-entry
                          (assoc table (cdr s-entry) :test 'equal)))
            (f-entry    (when t-entry
                          (assoc name (cdr t-entry) :test 'equal)))
            (fkey
             (make-pgsql-fkey :name name
                              :table-name (qualify-name schema table)
                              :columns (list col)
                              :foreign-table (qualify-name fschema ftable)
                              :foreign-columns (list fcol))))
       (if s-entry
           (if t-entry
               (if f-entry
                   (let ((fkey (cdr f-entry)))
                     (push col (pgloader.pgsql::pgsql-fkey-columns fkey))
                     (push fcol (pgloader.pgsql::pgsql-fkey-foreign-columns fkey)))
                   (push (cons name fkey) (cdr t-entry)))
               (push (cons table (list (cons name fkey))) (cdr s-entry)))
           (push (cons schema
                       (list (cons table
                                   (list (cons name fkey))))) result)))
     :finally
     ;; we did push, we need to reverse here
     (return
       (labels ((reverse-fkey-cols (fkey)
                  (setf (pgloader.pgsql::pgsql-fkey-columns fkey)
                        (nreverse (pgloader.pgsql::pgsql-fkey-columns fkey)))
                  (setf (pgloader.pgsql::pgsql-fkey-foreign-columns fkey)
                        (nreverse
                         (pgloader.pgsql::pgsql-fkey-foreign-columns fkey)))
                  fkey)

                (reverse-fkeys-cols (list-of-fkeys)
                  (loop :for (name . fkeys) :in list-of-fkeys
                     :collect (cons name (reverse-fkey-cols fkeys))))

                (reverse-fkeys-cols (list-of-tables)
                  (reverse
                   (loop :for (table . fkeys) :in list-of-tables
                      :collect (cons table (reverse-fkeys-cols fkeys))))))
         (reverse
          (loop :for (schema . tables) :in result
             :collect (cons schema (reverse-fkeys-cols tables))))))))


;;;
;;; Tools to handle row queries.
;;;
(defun get-column-sql-expression (name type)
  "Return per-TYPE SQL expression to use given a column NAME.

   Mostly we just use the name, and make try to avoid parsing dates."
  (case (intern (string-upcase type) "KEYWORD")
    (:datetime  (format nil "convert(varchar, [~a], 126)" name))
    (:bigint    (format nil "cast([~a] as numeric)" name))
    (t          (format nil "[~a]" name))))

(defun get-column-list (columns)
  "Tweak how we fetch the column values to avoid parsing when possible."
  (loop :for col :in columns
     :collect (with-slots (name type) col
                (get-column-sql-expression name type))))

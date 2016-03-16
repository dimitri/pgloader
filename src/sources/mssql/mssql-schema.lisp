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

(defmethod clone-connection ((c mssql-connection))
  (change-class (call-next-method c) 'mssql-connection))

(defmethod query ((msconn mssql-connection) sql &key)
  "Send SQL query to MSCONN connection."
  (mssql:query sql :connection (conn-handle msconn)))

(defun mssql-query (query)
  "Execute given QUERY within the current *connection*, and set proper
   defaults for pgloader."
  (log-message :debug "MSSQL: sending query: ~a" query)
  (mssql:query query :connection (conn-handle *mssql-db*)))


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

(defun list-all-columns (catalog
                         &key
			   (table-type :table)
                           including
                           excluding
			 &aux
			   (table-type-name (cdr (assoc table-type *table-type*))))
  (loop
     :for (schema-name table-name name type default nullable identity
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
         WHEN c.column_default LIKE '((%' AND c.column_default LIKE '%))' THEN
             CASE
                 WHEN SUBSTRING(c.column_default,3,len(c.column_default)-4) = 'newid()' THEN 'generate_uuid_v4()'
                 WHEN SUBSTRING(c.column_default,3,len(c.column_default)-4) LIKE 'convert(%varchar%,getdate(),%)' THEN 'today'
                 WHEN SUBSTRING(c.column_default,3,len(c.column_default)-4) = 'getdate()' THEN 'now'
                 WHEN SUBSTRING(c.column_default,3,len(c.column_default)-4) LIKE '''%''' THEN SUBSTRING(c.column_default,4,len(c.column_default)-6)
                 ELSE SUBSTRING(c.column_default,3,len(c.column_default)-4)
             END
         WHEN c.column_default LIKE '(%' AND c.column_default LIKE '%)' THEN
             CASE
                 WHEN SUBSTRING(c.column_default,2,len(c.column_default)-2) = 'newid()' THEN 'generate_uuid_v4()'
                 WHEN SUBSTRING(c.column_default,2,len(c.column_default)-2) LIKE 'convert(%varchar%,getdate(),%)' THEN 'today'
                 WHEN SUBSTRING(c.column_default,2,len(c.column_default)-2) = 'getdate()' THEN 'now'
                 WHEN SUBSTRING(c.column_default,2,len(c.column_default)-2) LIKE '''%''' THEN SUBSTRING(c.column_default,3,len(c.column_default)-4)
                 ELSE SUBSTRING(c.column_default,2,len(c.column_default)-2)
             END
         ELSE c.column_default
         END,
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
                          including     ; do we print the clause?
                          (filter-list-to-where-clause including
                                                       nil
                                                       "c.table_schema"
                                                       "c.table_name")
                          excluding     ; do we print the clause?
                          (filter-list-to-where-clause excluding
                                                       t
                                                       "c.table_schema"
                                                       "c.table_name")))
     :do
     (let* ((schema     (maybe-add-schema catalog schema-name))
            (table      (maybe-add-table schema table-name))
            (field
             (make-mssql-column
              schema-name table-name name type default nullable
              (eq 1 identity)
              character-maximum-length
              numeric-precision numeric-precision-radix numeric-scale
              datetime-precision
              character-set-name collation-name)))
       (add-field table field))
     :finally (return catalog)))

(defun list-all-indexes (catalog &key including excluding)
  "Get the list of MSSQL index definitions per table."
  (loop
     :for (schema-name table-name index-name col unique pkey)
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
     (let* ((schema     (find-schema catalog schema-name))
            (table      (find-table schema table-name))
            (index      (make-pgsql-index :name index-name
                                          :primary (= pkey 1)
                                          :unique (= unique 1)
                                          :columns (list col))))
       (add-index table index))
     :finally (return catalog)))

(defun list-all-fkeys (catalog &key including excluding)
  "Get the list of MSSQL index definitions per table."
  (loop
     :for (fkey-name schema-name table-name col fschema-name ftable-name fcol)
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
     (let* ((schema     (find-schema catalog schema-name))
            (table      (find-table schema table-name))
            (fschema    (find-schema catalog fschema-name))
            (ftable     (find-table fschema ftable-name))
            (pg-fkey
             (make-pgsql-fkey :name fkey-name
                              :table table
                              :columns (list col)
                              :foreign-table ftable
                              :foreign-columns (list fcol)))
            (fkey       (maybe-add-fkey table fkey-name pg-fkey
                                        :key #'pgloader.pgsql::pgsql-fkey-name)))
       (push-to-end col  (pgloader.pgsql::pgsql-fkey-columns fkey))
       (push-to-end fcol (pgloader.pgsql::pgsql-fkey-foreign-columns fkey)))
     :finally (return catalog)))


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

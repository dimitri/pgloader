;;;
;;; Tools to handle the MS SQL Database
;;;

(in-package :pgloader.mssql)

(defvar *mssql-db* nil
  "The MS SQL database connection handler.")

(defparameter *mssql-default-cast-rules*
  `((:source (:type "char")      :target (:type "text" :drop-typemod t))
    (:source (:type "nchar")     :target (:type "text" :drop-typemod t))
    (:source (:type "varchar")   :target (:type "text" :drop-typemod t))
    (:source (:type "nvarchar")  :target (:type "text" :drop-typemod t))
    (:source (:type "xml")       :target (:type "xml" :drop-typemod t))

    (:source (:type "bit") :target (:type "boolean"))

    (:source (:type "uniqueidentifier") :target (:type "uuid"))

    (:source (:type "money") :target (:type "numeric"))
    (:source (:type "smallmoney") :target (:type "numeric"))

    (:source (:type "tinyint") :target (:type "smallint"))

    (:source (:type "float") :target (:type "float")
             :using pgloader.transforms::float-to-string)

    (:source (:type "real") :target (:type "real")
             :using pgloader.transforms::float-to-string)

    (:source (:type "double") :target (:type "double precision")
             :using pgloader.transforms::float-to-string)

    (:source (:type "numeric") :target (:type "numeric")
             :using pgloader.transforms::float-to-string)

    (:source (:type "binary") :target (:type "bytea")
             :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "varbinary") :target (:type "bytea")
             :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "datetime") :target (:type "timestamptz")
             :using pgloader.transforms::sqlite-timestamp-to-timestamp))
  "Data Type Casting to migrate from MSSQL to PostgreSQL")

;;;
;;; General utility to manage MySQL connection
;;;
(defun mssql-query (query)
  "Execute given QUERY within the current *connection*, and set proper
   defaults for pgloader."
  (mssql:query query :connection *mssql-db*))

(defmacro with-mssql-connection ((&optional (dbname *ms-dbname*)) &body forms)
  "Connect to MSSQL, use given DBNAME as the current database if provided,
   and execute FORMS in a protected way so that we always disconnect when
   done.

   Connection parameters are *myconn-host*, *myconn-port*, *myconn-user* and
   *myconn-pass*."
  `(let* ((dbname      (or ,dbname *ms-dbname*))
          (*mssql-db*  (mssql:connect dbname
                                      *msconn-user*
                                      *msconn-pass*
                                      *msconn-host*)))
     (unwind-protect
          (progn ,@forms)
       (mssql:disconnect *mssql-db*))))


;;;
;;; Specific implementation of schema migration, see the API in
;;; src/pgsql/schema.lisp
;;;
(defstruct (mssql-column
	     (:constructor make-mssql-column
			   (schema table-name name type
                                   default nullable identity
                                   character-maximum-length
                                   numeric-precision
                                   numeric-precision-radix
                                   numeric-scale
                                   datetime-precision
                                   character-set-name
                                   collation-name)))
  schema table-name name type default nullable identity
  character-maximum-length
  numeric-precision numeric-precision-radix numeric-scale
  datetime-precision
  character-set-name collation-name)

(defmethod mssql-column-ctype ((col mssql-column))
  "Build the ctype definition from the full mssql-column information."
  (let ((type (mssql-column-type col)))
    (cond ((and (string= type "int")
                (mssql-column-identity col))
           "bigserial")

          ((member type
                   '("decimal" "numeric" "float" "double" "real")
                   :test #'string=)
           (format nil "~a(~a,~a)"
                   type
                   (mssql-column-numeric-precision col)
                   (mssql-column-numeric-scale col)))

          (t type))))

(defmethod format-pgsql-column ((col mssql-column) &key identifier-case)
  "Return a string representing the PostgreSQL column definition."
  (let* ((column-name
	  (apply-identifier-case (mssql-column-name col) identifier-case))
	 (type-definition
	  (with-slots (schema table-name name type default nullable)
	      col
            (declare (ignore schema))   ; FIXME
            (let ((ctype (mssql-column-ctype col)))
              (cast table-name name type ctype default nullable nil)))))
    (format nil "~a ~22t ~a" column-name type-definition)))


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

(defun list-all-columns (&key
                           (dbname *ms-dbname*)
			   (table-type :table)
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
         c.column_default,
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

order by table_schema, table_name, ordinal_position"
                          dbname
                          table-type-name))
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

(defun list-all-indexes (&key
                           (dbname *my-dbname*))
  "Get the list of MSSQL index definitions per table."
  (loop
     :with result := nil
     :for (schema table-name name unique col)
     :in
     (mssql:query (format nil "
SELECT OBJECT_SCHEMA_NAME(T.[object_id],DB_ID()) AS [Schema],
  T.[name] AS [table_name], I.[name] AS [index_name], AC.[name] AS [column_name],
  I.[type_desc], I.[is_unique], I.[data_space_id], I.[ignore_dup_key], I.[is_primary_key], 
  I.[is_unique_constraint], I.[fill_factor],    I.[is_padded], I.[is_disabled], I.[is_hypothetical], 
  I.[allow_row_locks], I.[allow_page_locks], IC.[is_descending_key], IC.[is_included_column] 
FROM sys.[tables] AS T  
  INNER JOIN sys.[indexes] I ON T.[object_id] = I.[object_id]  
  INNER JOIN sys.[index_columns] IC ON I.[object_id] = IC.[object_id] 
  INNER JOIN sys.[all_columns] AC ON T.[object_id] = AC.[object_id] AND IC.[column_id] = AC.[column_id] 
WHERE T.[is_ms_shipped] = 0 AND I.[type_desc] <> 'HEAP' 
ORDER BY T.[name], I.[index_id], IC.[key_ordinal];")
                  :connection *mssql-db*)
     :do
     (let* ((s-entry    (assoc schema result :test 'equal))
            (i-entry    (when s-entry
                          (assoc table-name (cdr s-entry) :test 'equal)))
            (index
             ))
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

;;;
;;; Tools to handle the MS SQL Database
;;;

(in-package :pgloader.mssql)

(defvar *mssql-db* nil
  "The MS SQL database connection handler.")


;;;
;;; Specific implementation of schema migration, see the API in
;;; src/pgsql/schema.lisp
;;;
(defstruct (mssql-column
	     (:constructor make-mssql-column
			   (schema table-name name type default nullable
                                   character-maximum-length
                                   numeric-precision
                                   numeric-precision-radix
                                   numeric-scale
                                   datetime-precision
                                   character-set-name
                                   collation-name)))
  schema table-name name type default nullable
  character-maximum-length
  numeric-precision numeric-precision-radix numeric-scale
  datetime-precision
  character-set-name collation-name)


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
                           (dbname *my-dbname*)
			   (table-type :table)
			 &aux
			   (table-type-name (cdr (assoc table-type *table-type*))))
  (loop
     :with result := nil
     :for (schema table-name name type default nullable
                  character-maximum-length
                  numeric-precision numeric-precision-radix numeric-scale
                  datetime-precision
                  character-set-name collation-name)
     :in
     (mssql:query (format nil "
  select c.table_schema,
         c.table_name,
         c.column_name,
         c.data_type,
         c.column_default,
         c.is_nullable,
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
                          table-type-name)
                  :connection *mssql-db*)
     :do
     (let* ((s-entry    (assoc schema result :test 'equal))
            (t-entry    (when s-entry
                          (assoc table-name (cdr s-entry) :test 'equal)))
            (column
             (make-mssql-column
              schema table-name name type default nullable
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


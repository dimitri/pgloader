;;;
;;; Tools to query the MS SQL Schema to reproduce in PostgreSQL
;;;

(in-package :pgloader.source.mssql)

(defclass copy-mssql (db-copy)
  ((encoding :accessor encoding         ; allows forcing encoding
             :initarg :encoding
             :initform nil))
  (:documentation "pgloader MS SQL Data Source"))

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

(defmethod filter-list-to-where-clause ((mssql copy-mssql)
                                        filter-list
                                        &key
                                          not
                                          (schema-col "table_schema")
                                          (table-col  "table_name"))
  "Given an INCLUDING or EXCLUDING clause, turn it into a MS SQL WHERE clause."
  (loop :for (schema . table-name-list) :in filter-list
     :append (mapcar (lambda (table-name)
                       (format nil "(~a = '~a' and ~a ~:[~;NOT ~]LIKE '~a')"
                               schema-col schema table-col not table-name))
                     table-name-list)))

(defmethod fetch-columns ((catalog catalog)
                          (mssql copy-mssql)
                          &key
                            (table-type :table)
                            including
                            excluding
                          &aux
                            (table-type-name
                             (cdr (assoc table-type *table-type*))))
  (loop
     :with incl-where := (filter-list-to-where-clause
                          mssql including :not nil
                          :schema-col "c.table_schema"
                          :table-col "c.table_name")
     :with excl-where := (filter-list-to-where-clause
                          mssql excluding :not t
                          :schema-col "c.table_schema"
                          :table-col "c.table_name")
     :for (schema-name table-name name type default nullable identity
                       character-maximum-length
                       numeric-precision numeric-precision-radix numeric-scale
                       datetime-precision
                       character-set-name collation-name)
     :in (mssql-query (sql "/mssql/list-all-columns.sql"
                           (db-name *mssql-db*)
                           table-type-name
                           incl-where   ; do we print the clause?
                           incl-where
                           excl-where   ; do we print the clause?
                           excl-where))
     :do (let* ((schema     (maybe-add-schema catalog schema-name))
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

(defmethod fetch-indexes ((catalog catalog)
                          (mssql copy-mssql)
                          &key including excluding)
  "Get the list of MSSQL index definitions per table."
  (loop
     :with incl-where := (filter-list-to-where-clause
                          mssql including :not nil
                          :schema-col "schema_name(schema_id)"
                          :table-col "o.name")
     :with excl-where := (filter-list-to-where-clause
                          mssql excluding :not t
                          :schema-col "schema_name(schema_id)"
                          :table-col "o.name")
     :for (schema-name table-name index-name colname unique pkey filter)
     :in  (mssql-query (sql "/mssql/list-all-indexes.sql"
                            incl-where  ; do we print the clause?
                            incl-where
                            excl-where  ; do we print the clause?
                            excl-where))
     :do (let* ((schema     (find-schema catalog schema-name))
                (table      (find-table schema table-name))
                (pg-index   (make-index :name index-name
                                        :schema schema
                                        :table table
                                        :primary (= pkey 1)
                                        :unique (= unique 1)
                                        :columns nil
                                        :filter filter))
                (index
                 (when table
                   (maybe-add-index table index-name pg-index :key #'index-name))))
           (unless table
             (log-message :warning
                          "Failed to find table ~s in schema ~s for index ~s, skipping the index"
                          table-name schema-name index-name))
           (when index
             (add-column index colname)))
     :finally (return catalog)))

(defmethod fetch-foreign-keys ((catalog catalog) (mssql copy-mssql)
                               &key including excluding)
  "Get the list of MSSQL index definitions per table."
  (loop
     :with incl-where := (filter-list-to-where-clause
                          mssql including :not nil
                          :schema-col "kcu1.table_schema"
                          :table-col "kcu1.table_name")
     :with excl-where := (filter-list-to-where-clause
                          mssql excluding :not t
                          :schema-col "kcu1.table_schema"
                          :table-col "kcu1.table_name")
     :for (fkey-name schema-name table-name col
                     fschema-name ftable-name fcol
                     fk-update-rule fk-delete-rule)
     :in  (mssql-query (sql "/mssql/list-all-fkeys.sql"
                            (db-name *mssql-db*) (db-name *mssql-db*)
                            incl-where  ; do we print the clause?
                            incl-where
                            excl-where  ; do we print the clause?
                            excl-where))
     :do (let* ((schema    (find-schema catalog schema-name))
                (table     (find-table schema table-name))
                (fschema   (find-schema catalog fschema-name))
                (ftable    (find-table fschema ftable-name))
                (col-name  (apply-identifier-case col))
                (fcol-name (apply-identifier-case fcol))
                (pg-fkey
                 (make-fkey :name (apply-identifier-case fkey-name)
                            :table table
                            :columns nil
                            :foreign-table ftable
                            :foreign-columns nil
                            :update-rule fk-update-rule
                            :delete-rule fk-delete-rule))
                (fkey
                 (maybe-add-fkey table fkey-name pg-fkey :key #'fkey-name)))
           (push-to-end col-name (fkey-columns fkey))
           (push-to-end fcol-name (fkey-foreign-columns fkey)))
     :finally (return catalog)))


;;;
;;; Tools to handle row queries.
;;;
(defmethod get-column-sql-expression ((mssql copy-mssql) name type)
  "Return per-TYPE SQL expression to use given a column NAME.

   Mostly we just use the name, and make try to avoid parsing dates."
  (case (intern (string-upcase type) "KEYWORD")
    (:time           (format nil "convert(varchar(30), [~a], 114)" name))
    (:datetime       (format nil "convert(varchar(30), [~a], 126)" name))
    (:datetime2      (format nil "convert(varchar(30), [~a], 126)" name))
    (:datetimeoffset (format nil "convert(varchar(35), [~a], 127)" name))
    (:smalldatetime  (format nil "convert(varchar(30), [~a], 126)" name))
    (:date           (format nil "convert(varchar(30), [~a], 126)" name))
    (:bigint         (format nil "cast([~a] as numeric(20))" name))
    (t               (format nil "[~a]" name))))

(defmethod get-column-list ((mssql copy-mssql))
  "Tweak how we fetch the column values to avoid parsing when possible."
  (loop :for field :in (fields mssql)
     :collect (with-slots (name type) field
                (get-column-sql-expression mssql name type))))

;;;
;;; Tools to query the MS SQL Schema to reproduce in PostgreSQL
;;;

(in-package :pgloader.source.mssql)

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
  ;; apply mysql-settings, if any
  (loop :for (name . value) :in *mssql-settings*
     :for sql := (format nil "set ~a ~a;" name value)
     :do (query msconn sql))

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
  (log-message :sql "MSSQL: sending query: ~a" sql)
  (mssql:query sql :connection (conn-handle msconn)))

(defun mssql-query (query)
  "Execute given QUERY within the current *connection*, and set proper
   defaults for pgloader."
  (query *mssql-db* query))


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
     (mssql-query (format nil
                          (sql "/mssql/list-all-columns.sql")
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
     :for (schema-name table-name index-name colname unique pkey filter)
     :in  (mssql-query (format nil
                               (sql "/mssql/list-all-indexes.sql")
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

(defun list-all-fkeys (catalog &key including excluding)
  "Get the list of MSSQL index definitions per table."
  (loop
     :for (fkey-name schema-name table-name col
                     fschema-name ftable-name fcol
                     fk-update-rule fk-delete-rule)
     :in  (mssql-query (format nil
                               (sql "/mssql/list-all-fkeys.sql")
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
             (make-fkey :name fkey-name
                        :table table
                        :columns nil
                        :foreign-table ftable
                        :foreign-columns nil
                        :update-rule fk-update-rule
                        :delete-rule fk-delete-rule))
            (fkey
             (maybe-add-fkey table fkey-name pg-fkey :key #'fkey-name)))
       (push-to-end col  (fkey-columns fkey))
       (push-to-end fcol (fkey-foreign-columns fkey)))
     :finally (return catalog)))


;;;
;;; Tools to handle row queries.
;;;
(defun get-column-sql-expression (name type)
  "Return per-TYPE SQL expression to use given a column NAME.

   Mostly we just use the name, and make try to avoid parsing dates."
  (case (intern (string-upcase type) "KEYWORD")
    (:time           (format nil "convert(varchar, [~a], 114)" name))
    (:datetime       (format nil "convert(varchar, [~a], 126)" name))
    (:smalldatetime  (format nil "convert(varchar, [~a], 126)" name))
    (:date           (format nil "convert(varchar, [~a], 126)" name))
    (:bigint         (format nil "cast([~a] as numeric)" name))
    (t               (format nil "[~a]" name))))

(defun get-column-list (columns)
  "Tweak how we fetch the column values to avoid parsing when possible."
  (loop :for col :in columns
     :collect (with-slots (name type) col
                (get-column-sql-expression name type))))



;;;
;;; Materialize Views support
;;;
(defun create-ms-views (views-alist)
  "VIEWS-ALIST associates view names with their SQL definition, which might
   be empty for already existing views. Create only the views for which we
   have an SQL definition."
  (unless (eq :all views-alist)
    (let ((views (remove-if #'null views-alist :key #'cdr)))
      (when views
        (loop :for (name . def) :in views
           :for sql := (destructuring-bind (schema . v-name) name
                         (format nil
                                 "CREATE VIEW ~@[~s~].~s AS ~a"
                                 schema v-name def))
           :do (progn
                 (log-message :info "MS SQL: ~a" sql)
                 (mssql-query sql)))))))

(defun drop-ms-views (views-alist)
  "See `create-ms-views' for VIEWS-ALIST description. This time we DROP the
   views to clean out after our work."
  (unless (eq :all views-alist)
   (let ((views (remove-if #'null views-alist :key #'cdr)))
     (when views
       (let ((sql
              (with-output-to-string (sql)
                (format sql "DROP VIEW ")
                (loop :for view-definition :in views
                   :for i :from 0
                   :do (destructuring-bind (name . def) view-definition
                         (declare (ignore def))
                         (format sql
                                 "~@[, ~]~@[~s.~]~s"
                                 (not (zerop i)) (car name) (cdr name)))))))
         (log-message :info "PostgreSQL Source: ~a" sql)
         (mssql-query sql))))))

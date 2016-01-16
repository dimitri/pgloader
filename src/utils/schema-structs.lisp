;;;
;;; PostgreSQL catalogs data structures
;;;
;;; Advanced (database) pgloader data source have to provide facilities to
;;; introspect themselves and CAST their catalogs into PostgreSQL compatible
;;; catalogs as defined here.
;;;
;;; Utility function using those definitions are found in schema.lisp in the
;;; same directory.
;;;
(in-package :pgloader.schema)

(defmacro push-to-end (item place)
  `(setf ,place (nconc ,place (list ,item))))

;;;
;;; TODO: stop using anonymous data structures for database catalogs,
;;; currently list of alists of lists... the madness has found its way in
;;; lots of places tho.
;;;

;;;
;;; A database catalog is a list of schema each containing a list of tables,
;;; each being a list of columns.
;;;
;;; Column structures details depend on the specific source type and are
;;; implemented in each source separately.
;;;
(defstruct catalog name schema-list)
(defstruct schema source-name name table-list view-list)
(defstruct table source-name name schema oid comment
           ;; field is for SOURCE
           ;; column is for TARGET
           field-list column-list index-list fkey-list)

;;;
;;; The generic PostgreSQL column that the CAST generic function is asked to
;;; produce, so that we know how to CREATE TABLEs in PostgreSQL whatever the
;;; source is.
;;;
(defstruct column name type-name type-mod nullable default comment transform)

;;; those are currently defined in ./schema.lisp
;; (defstruct index name primary unique columns sql conname condef)
;; (defstruct fkey
;;   name columns foreign-table foreign-columns update-rule delete-rule)

;;;
;;; Main data collection API
;;;
(defgeneric add-schema  (object schema-name &key))
(defgeneric add-table   (object table-name &key))
(defgeneric add-view    (object view-name &key))
(defgeneric add-column  (object column &key))
(defgeneric add-index   (object index &key))
(defgeneric add-fkey    (object fkey &key))
(defgeneric add-comment (object comment &key))

(defgeneric table-list (object &key)
  (:documentation "Return the list of tables found in OBJECT."))

(defgeneric view-list (object &key)
  (:documentation "Return the list of views found in OBJECT."))

(defgeneric find-schema (object schema-name &key)
  (:documentation
   "Find a schema by SCHEMA-NAME in a catalog OBJECT and return the schema"))

(defgeneric find-table (object table-name &key)
  (:documentation
   "Find a table by TABLE-NAME in a schema OBJECT and return the table"))

(defgeneric find-view (object view-name &key)
  (:documentation
   "Find a table by TABLE-NAME in a schema OBJECT and return the table"))

(defgeneric find-fkey (object fkey-name &key key test)
  (:documentation
   "Find a table by FKEY-NAME in a table OBJECT and return the fkey"))

(defgeneric maybe-add-schema (object schema-name &key)
  (:documentation "Add a new schema or return existing one."))

(defgeneric maybe-add-table (object table-name &key)
  (:documentation "Add a new table or return existing one."))

(defgeneric maybe-add-view (object view-name &key)
  (:documentation "Add a new view or return existing one."))

(defgeneric maybe-add-fkey (object fkey-name fkey &key key test)
  (:documentation "Add a new fkey or return existing one."))

(defgeneric count-tables (object &key)
  (:documentation "Count how many tables we have in total in OBJECT."))

(defgeneric count-views (object &key)
  (:documentation "Count how many views we have in total in OBJECT."))

(defgeneric count-indexes (object &key)
  (:documentation "Count how many indexes we have in total in OBJECT."))

(defgeneric count-fkeys (object &key)
  (:documentation "Count how many forein keys we have in total in OBJECT."))

(defgeneric max-indexes-per-table (schema &key)
  (:documentation "Count how many indexes we have maximum per table in SCHEMA."))

(defgeneric cast (object)
  (:documentation
   "Cast a FIELD definition from a source database into a PostgreSQL COLUMN
    definition."))


;;;
;;; Implementation of the methods
;;;
(defmethod table-list ((schema schema) &key)
  "Return the list of tables for SCHEMA."
  (schema-table-list schema))

(defmethod table-list ((catalog catalog) &key)
  "Return the list of tables for table."
  (apply #'append (mapcar #'table-list (catalog-schema-list catalog))))

(defmethod view-list ((schema schema) &key)
  "Return the list of views for SCHEMA."
  (schema-view-list schema))

(defmethod view-list ((catalog catalog) &key)
  "Return the list of views for cATALOG."
  (apply #'append (mapcar #'view-list (catalog-schema-list catalog))))

(defun create-table (maybe-qualified-name)
  "Create a table instance from the db-uri component, either a string or a
   cons of two strings: (schema . table)."
  (typecase maybe-qualified-name
    (string (make-table :source-name maybe-qualified-name
                        :name (apply-identifier-case maybe-qualified-name)))

    (cons   (make-table :source-name maybe-qualified-name
                        :name (apply-identifier-case
                               (cdr maybe-qualified-name))
                        :schema (apply-identifier-case
                                 (car maybe-qualified-name))))))

(defmethod add-schema ((catalog catalog) schema-name &key)
  "Add SCHEMA-NAME to CATALOG and return the new schema instance."
  (let ((schema (make-schema :source-name schema-name
                             :name (when schema-name
                                     (apply-identifier-case schema-name)))))
    (push-to-end schema (catalog-schema-list catalog))
    schema))

(defmethod add-table ((schema schema) table-name &key comment)
  "Add TABLE-NAME to SCHEMA and return the new table instance."
  (let ((table
         (make-table :source-name table-name
                     :name (apply-identifier-case table-name)
                     :schema (schema-name schema)
                     :comment (unless (or (null comment) (string= "" comment))
                                comment))))
    (push-to-end table (schema-table-list schema))
    table))

(defmethod add-view ((schema schema) view-name &key comment)
  "Add TABLE-NAME to SCHEMA and return the new table instance."
  (let ((view
         (make-table :source-name view-name
                     :name (apply-identifier-case view-name)
                     :schema (schema-name schema)
                     :comment (unless (or (null comment) (string= "" comment))
                                comment))))
    (push-to-end view (schema-view-list schema))
    view))

(defmethod find-schema ((catalog catalog) schema-name &key)
  "Find SCHEMA-NAME in CATALOG and return the SCHEMA object of this name."
  (find schema-name (catalog-schema-list catalog)
        :key #'schema-source-name :test 'string=))

(defmethod find-table ((schema schema) table-name &key)
  "Find TABLE-NAME in SCHEMA and return the TABLE object of this name."
  (find table-name (schema-table-list schema)
        :key #'table-source-name :test 'string=))

(defmethod find-view ((schema schema) view-name &key)
  "Find TABLE-NAME in SCHEMA and return the TABLE object of this name."
  (find view-name (schema-view-list schema)
        :key #'table-source-name :test 'string=))

(defmethod maybe-add-schema ((catalog catalog) schema-name &key)
  "Add SCHEMA-NAME to the schema-list for CATALOG, or return the existing
   schema of the same name if it already exists in the catalog schema-list"
  (let ((schema (find-schema catalog schema-name)))
    (or schema (add-schema catalog schema-name))))

(defmethod maybe-add-table ((schema schema) table-name &key comment)
  "Add TABLE-NAME to the table-list for SCHEMA, or return the existing table
   of the same name if it already exists in the schema table-list."
  (let ((table (find-table schema table-name)))
    (or table (add-table schema table-name :comment comment))))

(defmethod maybe-add-view ((schema schema) view-name &key comment)
  "Add TABLE-NAME to the table-list for SCHEMA, or return the existing table
   of the same name if it already exists in the schema table-list."
  (let ((table (find-view schema view-name)))
    (or table (add-view schema view-name :comment comment))))

(defmethod add-field ((table table) field &key)
  "Add COLUMN to TABLE and return the TABLE."
  (push-to-end field (table-field-list table))
  table)

(defmethod add-column ((table table) column &key)
  "Add COLUMN to TABLE and return the TABLE."
  (push-to-end column (table-column-list table))
  table)

(defmethod cast ((table table))
  "Cast all fields in table into columns."
  (setf (table-column-list table) (mapcar #'cast (table-field-list table))))

(defmethod cast ((schema schema))
  "Cast all fields of all tables in SCHEMA into columns."
  (loop :for table :in (schema-table-list schema)
     :do (cast table))

  (loop :for view :in (schema-view-list schema)
     :do (cast view)))

(defmethod cast ((catalog catalog))
  "Cast all fields of all tables in all schemas in CATALOG into columns."
  (loop :for schema :in (catalog-schema-list catalog)
     :do (cast schema)))

(defmethod add-index ((table table) index &key)
  "Add INDEX to TABLE and return the TABLE."
  (push-to-end index (table-index-list table)))

(defmethod add-fkey ((table table) fkey &key)
  "Add FKEY to TABLE and return the TABLE."
  (push-to-end fkey (table-fkey-list table)))

;;;
;;; There's no simple equivalent to array_agg() in MS SQL, so the fkey query
;;; returns a row per fkey column rather than per fkey. Hence this extra
;;; API:
;;;
(defmethod find-fkey ((table table) fkey-name &key key (test #'string=))
  "Find FKEY-NAME in TABLE and return the FKEY object of this name."
  (find fkey-name (table-fkey-list table) :key key :test test))

(defmethod maybe-add-fkey ((table table) fkey-name fkey &key key test)
  "Add the foreign key FKEY to the table-fkey-list of TABLE unless it
  already exists, and return the FKEY object."
  (let ((fkey (find-fkey table fkey-name :key key :test test)))
    (or fkey (add-fkey table fkey))))


;;;
;;; To report stats to the user, count how many objects we are taking care
;;; of.
;;;
(defmethod count-tables ((schema schema) &key)
  "Count tables in given SCHEMA."
  (length (schema-table-list schema)))

(defmethod count-tables ((catalog catalog) &key)
  (reduce #'+ (mapcar #'count-tables (catalog-schema-list catalog))))

(defmethod count-views ((schema schema) &key)
  "Count tables in given SCHEMA."
  (length (schema-view-list schema)))

(defmethod count-views ((catalog catalog) &key)
  (reduce #'+ (mapcar #'count-views (catalog-schema-list catalog))))

(defmethod count-indexes ((table table) &key)
  "Count indexes in given TABLE."
  (length (table-index-list table)))

(defmethod count-indexes ((schema schema) &key)
  "Count indexes in given SCHEMA."
  (reduce #'+ (mapcar #'count-indexes (schema-table-list schema))))

(defmethod count-indexes ((catalog catalog) &key)
  "Count indexes in given SCHEMA."
  (reduce #'+ (mapcar #'count-indexes (catalog-schema-list catalog))))

(defmethod count-fkeys ((table table) &key)
  "Count fkeys in given TABLE."
  (length (table-fkey-list table)))

(defmethod count-fkeys ((schema schema) &key)
  "Count fkeys in given SCHEMA."
  (reduce #'+ (mapcar #'count-fkeys (schema-table-list schema))))

(defmethod count-fkeys ((catalog catalog) &key)
  "Count fkeys in given SCHEMA."
  (reduce #'+ (mapcar #'count-fkeys (catalog-schema-list catalog))))

(defmethod max-indexes-per-table ((schema schema) &key)
  "Count how many indexes maximum per table are listed in SCHEMA."
  (reduce #'max (mapcar #'length
                        (mapcar #'table-index-list
                                (schema-table-list schema)))))
"Count how many indexes maximum per table are listed in SCHEMA."

(defmethod max-indexes-per-table ((catalog catalog) &key)
  "Count how many indexes maximum per table are listed in SCHEMA."
  (reduce #'max (mapcar #'max-indexes-per-table (catalog-schema-list catalog))))

;;;
;;; Not a generic/method because only used for the table object, and we want
;;; to use the usual structure print-method in stack traces.
;;;
(defgeneric format-table-name (object)
  (:documentation "Format the OBJECT name for PostgreSQL."))

(defmethod format-table-name ((table table))
  "TABLE should be a table instance, but for hysterical raisins might be a
   CONS of a schema name and a table name, or just the table name as a
   string."
  (format nil "~@[~a.~]~a" (table-schema table) (table-name table)))


;;;
;;; Still lacking round tuits here, so for the moment the representation of
;;; a table name is either a string or a cons built from schema and
;;; table-name.
;;;
(defmacro with-schema ((var table-name) &body body)
  "When table-name is a CONS, SET search_path TO its CAR and return its CDR,
   otherwise just return the TABLE-NAME. A PostgreSQL connection must be
   established when calling this function."
  `(let ((,var
          (typecase ,table-name
            (table  (if (table-schema ,table-name)
                        (let ((sql (format nil "SET search_path TO ~a;"
                                           (table-schema ,table-name))))
                          (log-message :notice "~a" sql)
                          (pgloader.pgsql:pgsql-execute sql)
                          (table-name ,table-name))
                        (table-name ,table-name)))
            (cons   (let ((sql (format nil "SET search_path TO ~a;"
                                       (car ,table-name))))
                      (log-message :notice "~a" sql)
                      (pgloader.pgsql:pgsql-execute sql)
                      (cdr ,table-name)))
            (string ,table-name))))
     ,@body))



;;;
;;; PostgreSQL column and type output
;;;
(defun format-default-value (default &optional transform)
  "Returns suitably quoted default value for CREATE TABLE command."
  (cond
    ((null default) "NULL")
    ((and (stringp default) (string= "NULL" default)) default)
    ((and (stringp default) (string= "CURRENT_TIMESTAMP" default)) default)
    ((and (stringp default) (string= "CURRENT TIMESTAMP" default))
     "CURRENT_TIMESTAMP")
    ((and (stringp default) (string= "newsequentialid()" default))
     "uuid_generate_v1()")
    (t
     ;; apply the transformation function to the default value
     (if transform (format-default-value (funcall transform default))
	 (format nil "'~a'" default)))))

(defmethod format-column ((column column))
  "Format the PostgreSQL data type."
  (with-slots (name type-name type-mod nullable default transform) column
    (format nil
            "~a ~22t ~a~:[~*~;~a~]~:[ not null~;~]~:[~; default ~a~]"
            name
            type-name
            type-mod
            type-mod
            nullable
            default default)))

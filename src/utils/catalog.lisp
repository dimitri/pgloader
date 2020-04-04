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
(in-package :pgloader.catalog)

;;;
;;; A macro to ease writing the catalog handling API
;;;
(defmacro push-to-end (item place)
  `(progn
     (setf ,place (nconc ,place (list ,item)))
     ;; and return the item we just pushed at the end of the place
     ,item))

;;;
;;; One interesting thing to do to all those catalog objects is to be able
;;; to print the DDL commands out: CREATE and DROP SQL statements.
;;;
(defgeneric format-create-sql (object &key stream if-not-exists)
  (:documentation "Generate proper SQL command to create OBJECT in
  PostgreSQL. The output is written to STREAM."))

(defgeneric format-drop-sql (object &key stream cascade if-exists)
  (:documentation "Generate proper SQL command to drop OBJECT in PostgreSQL.
  The output is written to STREAM."))

(defgeneric format-default-value (column &key stream)
  (:documentation "Generate proper value to be used as a default value for
  given COLUMN in PostgreSQL. The output is written to STREAM."))

;;;
;;; A database catalog is a list of schema each containing a list of tables,
;;; each being a list of columns.
;;;
;;; Column structures details depend on the specific source type and are
;;; implemented in each source separately.
;;;
(defstruct catalog name schema-list types-without-btree distribution-rules)

(defstruct schema source-name name catalog in-search-path
           table-list view-list matview-list extension-list sqltype-list)

(defstruct table source-name name schema oid comment
           storage-parameter-list tablespace
           (row-count-estimate 0 :type fixnum)
           ;; field is for SOURCE
           field-list
           ;; column is for TARGET
           column-list
           index-list
           fkey-list
           trigger-list
           ;; citus is an extra slot for citus support
           citus-rule)

(defstruct matview source-name name schema definition)

;;;
;;; When migrating from PostgreSQL to PostgreSQL we might have to install
;;; extensions to have data type coverage.
;;;
(defstruct extension name schema)

;;;
;;; When migrating from another database to PostgreSQL some data types might
;;; need to be tranformed dynamically into User Defined Types: ENUMs, SET,
;;; etc.
;;;
(defstruct sqltype name schema type source-def extra extension)

;;;
;;; The generic PostgreSQL column that the CAST generic function is asked to
;;; produce, so that we know how to CREATE TABLEs in PostgreSQL whatever the
;;; source is.
;;;
(defstruct column table name type-name type-mod nullable default comment
           transform extra (transform-default t))

;;;
;;; Index and Foreign Keys
;;;
(defstruct fkey
  name oid table columns pkey foreign-table foreign-columns condef
  update-rule delete-rule match-rule deferrable initially-deferred)

;;;
;;; An index, that might be underlying a e.g. UNIQUE constraint conname, in
;;; which case we need to use condef to build the index again from its
;;; definition, and drop the conname to drop the index.
;;;
;;; Also, primary keys might be dependencies of foreign keys, including ones
;;; that are out of scope for our load specifications and hence, catalog. We
;;; keep track of them in fk-deps so that we know to remove them then
;;; install them again at proper times.
;;;
(defstruct index
  name oid schema table type primary unique
  columns sql conname condef filter fk-deps)

;;;
;;; Triggers and trigger procedures, no args support (yet?)
;;;
(defstruct trigger name table action procedure)

(defstruct procedure schema name returns language body)

;;;
;;; Main data collection API
;;;
(defgeneric add-schema    (object schema-name &key))
(defgeneric add-extension (object extension-name &key))
(defgeneric add-table     (object table-name &key))
(defgeneric add-view      (object view-name &key))
(defgeneric add-sqltype   (object column &key))
(defgeneric add-column    (object column &key))
(defgeneric add-index     (object index &key))
(defgeneric add-fkey      (object fkey &key))
(defgeneric add-comment   (object comment &key))

(defgeneric extension-list (object &key)
  (:documentation "Return the list of extensions found in OBJECT."))

(defgeneric table-list (object &key)
  (:documentation "Return the list of tables found in OBJECT."))

(defgeneric view-list (object &key)
  (:documentation "Return the list of views found in OBJECT."))

(defgeneric find-schema (object schema-name &key)
  (:documentation
   "Find a schema by SCHEMA-NAME in a catalog OBJECT and return the schema"))

(defgeneric find-extension (object extension-name &key)
  (:documentation
   "Find an extension by EXTENSION-NAME in a schema OBJECT and return the table"))

(defgeneric find-table (object table-name &key)
  (:documentation
   "Find a table by TABLE-NAME in a schema OBJECT and return the table"))

(defgeneric find-view (object view-name &key)
  (:documentation
   "Find a table by TABLE-NAME in a schema OBJECT and return the table"))

(defgeneric find-index (object index-name &key key test)
  (:documentation
   "Find an index by INDEX-NAME in a table OBJECT and return the index"))

(defgeneric find-fkey (object fkey-name &key key test)
  (:documentation
   "Find a foreign key by FKEY-NAME in a table OBJECT and return the fkey"))

(defgeneric maybe-add-schema (object schema-name &key)
  (:documentation "Add a new schema or return existing one."))

(defgeneric maybe-add-extension (object extension-name &key)
  (:documentation "Add a new extension or return existing one."))

(defgeneric maybe-add-table (object table-name &key)
  (:documentation "Add a new table or return existing one."))

(defgeneric maybe-add-view (object view-name &key)
  (:documentation "Add a new view or return existing one."))

(defgeneric maybe-add-index (object index-name index &key key test)
  (:documentation "Add a new index or return existing one."))

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

(defgeneric cast (object &key)
  (:documentation
   "Cast a FIELD definition from a source database into a PostgreSQL COLUMN
    definition."))

(defgeneric field-name (object &key)
  (:documentation "Get the source database column name, or field-name."))


;;;
;;; Implementation of the methods
;;;
(defmethod extension-list ((schema schema) &key)
  "Return the list of extensions for SCHEMA."
  (schema-extension-list schema))

(defmethod extension-list ((catalog catalog) &key)
  "Return the list of extensions for CATALOG."
  (apply #'append (mapcar #'extension-list (catalog-schema-list catalog))))

(defmethod sqltype-list ((column column) &key)
  "Return the list of sqltypes for SCHEMA."
  (when (typep (column-type-name column) 'sqltype)
    (column-type-name column)))

(defmethod sqltype-list ((table table) &key)
  "Return the list of sqltypes for SCHEMA."
  (mapcar #'sqltype-list (table-column-list table)))

(defmethod sqltype-list ((schema schema) &key)
  "Return the list of sqltypes for SCHEMA."
  (append (schema-sqltype-list schema)
          (apply #'append
                 (mapcar #'sqltype-list (schema-table-list schema)))))

(defmethod sqltype-list ((catalog catalog) &key)
  "Return the list of sqltypes for CATALOG."
  (remove-duplicates
   (remove-if #'null
              (apply #'append
                     (mapcar #'sqltype-list (catalog-schema-list catalog))))
   :test #'string-equal :key #'sqltype-name))

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
  (etypecase maybe-qualified-name
    (string (make-table :source-name maybe-qualified-name
                        :name (apply-identifier-case maybe-qualified-name)))

    (cons   (make-table :source-name maybe-qualified-name
                        :name (apply-identifier-case
                               (cdr maybe-qualified-name))
                        :schema
                        (let ((sname (car maybe-qualified-name)))
                          (make-schema :catalog nil
                                       :source-name sname
                                       :name (apply-identifier-case sname)))))

    ;; some code path using pgloader as an API might end-up here with an
    ;; already cooked table structure, try it and see...
    (table maybe-qualified-name)))

(defmethod add-schema ((catalog catalog) schema-name &key in-search-path)
  "Add SCHEMA-NAME to CATALOG and return the new schema instance."
  (let ((schema (make-schema :catalog catalog
                             :source-name schema-name
                             :name (when schema-name
                                     (apply-identifier-case schema-name))
                             :in-search-path in-search-path)))
    (push-to-end schema (catalog-schema-list catalog))))

(defmethod add-extension ((schema schema) extension-name &key)
  "Add EXTENSION-NAME to SCHEMA and return the new extension instance."
  (let ((extension
         (make-extension :name extension-name
                         :schema schema)))
    (push-to-end extension (schema-extension-list schema))))

(defmethod add-sqltype ((schema schema) sqltype &key)
  "Add SQLTYPE instance to SCHEMA and return SQLTYPE."
  (push-to-end sqltype (schema-sqltype-list schema)))

(defmethod add-table ((schema schema) table-name &key comment oid)
  "Add TABLE-NAME to SCHEMA and return the new table instance."
  (let ((table
         (make-table :source-name table-name
                     :name (apply-identifier-case table-name)
                     :schema schema
                     :oid oid
                     :comment (unless (or (null comment) (string= "" comment))
                                comment))))
    (push-to-end table (schema-table-list schema))))

(defmethod add-view ((schema schema) view-name &key comment)
  "Add TABLE-NAME to SCHEMA and return the new table instance."
  (let ((view
         (make-table :source-name view-name
                     :name (apply-identifier-case view-name)
                     :schema schema
                     :comment (unless (or (null comment) (string= "" comment))
                                comment))))
    (push-to-end view (schema-view-list schema))))

(defmethod find-schema ((catalog catalog) schema-name &key)
  "Find SCHEMA-NAME in CATALOG and return the SCHEMA object of this name."
  (find schema-name (catalog-schema-list catalog)
        :key #'schema-source-name :test 'string=))

(defmethod find-extension ((schema schema) extension-name &key)
  "Find EXTENSION-NAME in SCHEMA and return the EXTENSION object of this name."
  (find extension-name (schema-extension-list schema)
        :key #'extension-name :test 'string=))

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

(defmethod maybe-add-extension ((schema schema) extension-name &key)
  "Add TABLE-NAME to the table-list for SCHEMA, or return the existing table
   of the same name if it already exists in the schema table-list."
  (let ((extension (find-extension schema extension-name)))
    (or extension (add-extension schema extension-name))))

(defmethod maybe-add-table ((schema schema) table-name &key comment oid)
  "Add TABLE-NAME to the table-list for SCHEMA, or return the existing table
   of the same name if it already exists in the schema table-list."
  (let ((table (find-table schema table-name)))
    (or table (add-table schema table-name :oid oid :comment comment))))

(defmethod maybe-add-view ((schema schema) view-name &key comment)
  "Add TABLE-NAME to the table-list for SCHEMA, or return the existing table
   of the same name if it already exists in the schema table-list."
  (let ((table (find-view schema view-name)))
    (or table (add-view schema view-name :comment comment))))

(defmethod add-field ((table table) field &key)
  "Add COLUMN to TABLE and return the TABLE."
  (push-to-end field (table-field-list table)))

(defmethod add-column ((table table) column &key)
  "Add COLUMN to TABLE and return the TABLE."
  (push-to-end column (table-column-list table)))

(defmethod add-column ((index index) column &key)
  "Add COLUMN name to INDEX and return the INDEX."
  (push-to-end (apply-identifier-case column) (index-columns index)))

(defmethod cast ((table table) &key)
  "Cast all fields in table into columns."
  (setf (table-column-list table)
        (loop :for field :in (table-field-list table)
         :collect (cast field :table table))))

(defmethod cast ((schema schema) &key)
  "Cast all fields of all tables in SCHEMA into columns."
  (loop :for table :in (schema-table-list schema)
     :do (cast table))

  (loop :for view :in (schema-view-list schema)
     :do (cast view)))

(defmethod cast ((catalog catalog) &key)
  "Cast all fields of all tables in all schemas in CATALOG into columns."
  (loop :for schema :in (catalog-schema-list catalog)
     :do (cast schema)))

(defmethod field-name ((column column) &key)
  (column-name column))

;;;
;;; There's no simple equivalent to array_agg() in MS SQL, so the index and
;;; fkey queries return a row per index|fkey column rather than per
;;; index|fkey. Hence this extra API:
;;;
(defmethod add-index ((table table) index &key)
  "Add INDEX to TABLE and return the TABLE."
  (push-to-end index (table-index-list table)))

(defmethod find-index ((table table) index-name &key key (test #'string=))
  "Find INDEX-NAME in TABLE and return the INDEX object of this name."
  (find index-name (table-index-list table) :key key :test test))

(defmethod maybe-add-index ((table table) index-name index &key key (test #'string=))
  "Add the index INDEX to the table-index-list of TABLE unless it already
   exists, and return the INDEX object."
  (let ((current-index (find-index table index-name :key key :test test)))
    (or current-index (add-index table index))))

(defmethod add-fkey ((table table) fkey &key)
  "Add FKEY to TABLE and return the TABLE."
  (push-to-end fkey (table-fkey-list table)))

(defmethod find-fkey ((table table) fkey-name &key key (test #'string=))
  "Find FKEY-NAME in TABLE and return the FKEY object of this name."
  (find fkey-name (table-fkey-list table) :key key :test test))

(defmethod maybe-add-fkey ((table table) fkey-name fkey &key key (test #'string=))
  "Add the foreign key FKEY to the table-fkey-list of TABLE unless it
  already exists, and return the FKEY object."
  (let ((current-fkey (find-fkey table fkey-name :key key :test test)))
    (or current-fkey (add-fkey table fkey))))


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
                                (schema-table-list schema)))
          :initial-value 0))
"Count how many indexes maximum per table are listed in SCHEMA."

(defmethod max-indexes-per-table ((catalog catalog) &key)
  "Count how many indexes maximum per table are listed in SCHEMA."
  (reduce #'max (mapcar #'max-indexes-per-table (catalog-schema-list catalog))
          :initial-value 0))

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
  (format nil "~@[~a.~]~a"
          (when (table-schema table) (schema-name (table-schema table)))
          (table-name table)))


(defmacro with-schema ((var table-name) &body body)
  "When table-name is a CONS, SET search_path TO its CAR and return its CDR,
   otherwise just return the TABLE-NAME. A PostgreSQL connection must be
   established when calling this function."
  (let ((schema-name (gensym "SCHEMA-NAME")))
    `(let* ((,schema-name (when (table-schema ,table-name)
                            (schema-name (table-schema ,table-name))))
            (,var
             (progn
               (if ,schema-name
                   (let ((sql (format nil "SET search_path TO ~a;" ,schema-name)))
                     (pgloader.pgsql:pgsql-execute sql)))
               (table-name ,table-name))))
       ,@body)))


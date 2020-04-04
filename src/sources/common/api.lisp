;;;
;;; Generic API for pgloader sources
;;;
(in-package :pgloader.sources)

;; Abstract classes to define the data loading API with
;;
;; The source name might be a table name (database server source) or a
;; filename (csv, dbase, etc), or something else entirely, like e.g. mongodb
;; collection.
(defclass copy ()
  ((source-db  :accessor source-db	; source database name
	       :initarg :source-db)
   (target-db  :accessor target-db	; target database name
	       :initarg :target-db)	;
   (source     :accessor source		; source name
	       :initarg :source)	;
   (target     :accessor target		; target table name
	       :initarg :target)	;
   (fields     :accessor fields		; list of fields, or nil
	       :initarg :fields)	;
   (columns    :accessor columns	; list of columns, or nil
	       :initarg :columns)	;
   (transforms :accessor transforms	; per-column transform functions
	       :initarg :transforms)    ;
   (process-fn :accessor preprocessor   ; pre-processor function
               :initarg :process-fn)    ;
   (format     :accessor copy-format    ; format can be :COPY
               :initarg :format         ; in which case no escaping
               :initform :raw))         ; has to be done
  (:documentation "pgloader Generic Data Source"))

(defmethod initialize-instance :after ((source copy) &key)
  "Add a default value for transforms in case it's not been provided."
  (when (slot-boundp source 'columns)
    (let ((transforms (when (slot-boundp source 'transforms)
			(slot-value source 'transforms))))
      (unless transforms
	(setf (slot-value source 'transforms)
              (make-list (length (slot-value source 'columns))))))))

(defgeneric concurrency-support (copy concurrency)
  (:documentation
   "Returns nil when no concurrency is supported, or a list of copy ojbects
    prepared to run concurrently."))

(defgeneric map-rows (source &key process-row-fn)
  (:documentation
   "Load data from SOURCE and funcall PROCESS-ROW-FUN for each row found in
    the SOURCE data. Each ROW is passed as a vector of objects."))

(defgeneric proprocess-row (source)
  (:documentation
   "Some source readers have pre-processing to do on the raw data, such as
    CSV user-defined field projections to PostgreSQL columns. This function
    returns the pre-processing function, which must be a funcallable object."))

(defgeneric queue-raw-data (source queue concurrency)
  (:documentation "Send raw data from the reader to the worker queue."))

(defgeneric data-is-preformatted-p (source)
  (:documentation
   "Process raw data from RAW-QUEUE and prepare batches of formatted text to
    send down to PostgreSQL with the COPY protocol in FORMATTED-QUEUE."))

(defgeneric copy-column-list (source)
  (:documentation
   "Return the list of column names for the data sent in the queue."))




;;;
;;; Class hierarchy allowing to share features among a subcategory of
;;; pgloader sources. Those subcategory are divided in about the same set as
;;; the connection types.
;;;
;;;   fd-connection: single file reader, copy
;;;   md-connection: multiple file reader, md-copy
;;;   db-connection: database connection reader, with introspection, db-copy
;;;
;;; Of those only md-copy objects share a lot in common, so we have another
;;; layer of protocols just for them here, and the shared implementation
;;; lives in md-methods.lisp in this directory.
;;;

(defclass md-copy (copy)
  ((encoding    :accessor encoding	  ; file encoding
	        :initarg :encoding)	  ;
   (skip-lines  :accessor skip-lines	  ; skip firt N lines
	        :initarg :skip-lines	  ;
		:initform 0)		  ;
   (header      :accessor header          ; CSV headers are col names
                :initarg :header          ;
                :initform nil))           ;
  (:documentation "pgloader Multiple Files Data Source (csv, fixed, copy)."))

(defgeneric parse-header (md-copy)
  (:documentation "Parse the file header and return a list of fields."))

(defgeneric process-rows (md-copy stream process-fn)
  (:documentation "Process rows from a given input stream."))

(defgeneric clone-copy-for (md-copy path-spec)
  (:documentation "Create a new instance for copying PATH-SPEC data."))


;;;
;;; Class hierarchy allowing to share features for database sources, where
;;; we do introspection to prepare an internal catalog and then cast that
;;; catalog to PostgreSQL before copying the data over.
;;;
(defclass db-copy (copy) ()
  (:documentation "pgloader Database Data Source (MySQL, SQLite, MS SQL)."))

(defgeneric fetch-metadata (db-copy catalog
                            &key
                              materialize-views
                              create-indexes
                              foreign-keys
                              including
                              excluding))

(defgeneric cleanup (db-copy catalog &key materialize-views)
  (:documentation "Clean-up after prepare-pgsql-database failure."))

(defgeneric instanciate-table-copy-object (db-copy table)
  (:documentation "Create a new instance for copying TABLE data."))

;;;
;;; Database source schema introspection API
;;;
;;; The methods for those function query the source database catalogs and
;;; populate pgloader's internal representation of its catalog.
;;;
;;; On some source systems (such as MySQL) a single schema can be adressed
;;; at a time, and the catalog object might be a schema directly.
;;;
(defgeneric filter-list-to-where-clause (db-copy filter-list
                                         &key
                                           not
                                           schema-col
                                           table-col)
  (:documentation "Transform a filter-list into SQL expression for DB-COPY."))

(defgeneric fetch-columns (catalog db-copy &key table-type including excluding)
  (:documentation
   "Get the list of schema, tables and columns from the source database."))

(defgeneric fetch-indexes (catalog db-copy &key including excluding)
  (:documentation "Get the list of indexes from the source database."))

(defgeneric fetch-foreign-keys (catalog db-copy &key including excluding)
  (:documentation "Get the list of foreign keys from the source database."))

(defgeneric fetch-table-row-count (catalog db-copy &key including excluding)
  (:documentation "Retrieve and set the row count estimate for given tables."))

(defgeneric fetch-comments (catalog db-copy &key including excluding)
  (:documentation "Get the list of comments from the source database."))

;;;
;;; We're going to generate SELECT * FROM table; queries to fetch the data
;;; and COPY it to the PostgreSQL target database. In reality we don't use
;;; SELECT *, and in many interesting cases we have to generate some SQL
;;; expression to fetch the source values in a format we can then either
;;; process in pgloader or just send-over as-is to Postgres.
;;;
(defgeneric get-column-sql-expression (db-copy name type)
  (:documentation
   "Generate SQL expression for the SELECT clause for given column."))

(defgeneric get-column-list (copy-db)
  (:documentation
   "Generate the SQL projection column list for the SELECT clause."))

;;;
;;; Materialized Views support
;;;
(defgeneric format-matview-name (matview copy)
  (:documentation "Format the materialized view name."))

(defgeneric create-matviews (matview-list db-copy)
  (:documentation "Create Materialized Views."))

(defgeneric drop-matviews (matview-list db-copy)
  (:documentation "Drop Materialized Views."))


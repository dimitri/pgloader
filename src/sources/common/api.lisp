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
	       :initarg :transforms))	;
  (:documentation "pgloader Generic Data Source"))

(defmethod initialize-instance :after ((source copy) &key)
  "Add a default value for transforms in case it's not been provided."
  (when (slot-boundp source 'columns)
    (let ((transforms (when (slot-boundp source 'transforms)
			(slot-value source 'transforms))))
      (unless transforms
	(setf (slot-value source 'transforms)
              (make-list (length (slot-value source 'columns))))))))

(defgeneric map-rows (source &key process-row-fn)
  (:documentation
   "Load data from SOURCE and funcall PROCESS-ROW-FUN for each row found in
    the SOURCE data. Each ROW is passed as a vector of objects."))

(defgeneric proprocess-row (source)
  (:documentation
   "Some source readers have pre-processing to do on the raw data, such as
    CSV user-defined field projections to PostgreSQL columns. This function
    returns the pre-processing function, which must be a funcallable object."))

(defgeneric queue-raw-data (source queue)
  (:documentation "Send raw data from the reader to the worker queue."))

(defgeneric format-data-to-copy (source raw-queue formatted-queue
                                 &optional pre-formatted)
  (:documentation
   "Process raw data from RAW-QUEUE and prepare batches of formatted text to
    send down to PostgreSQL with the COPY protocol in FORMATTED-QUEUE."))

(defgeneric copy-column-list (source)
  (:documentation
   "Return the list of column names for the data sent in the queue."))

(defgeneric copy-from (source &key truncate)
  (:documentation
   "Load data from SOURCE into its target as defined by the SOURCE object."))

;; That one is more an export than a load. It always export to a single very
;; well defined format, the importing utility is defined in
;; src/pgsql-copy-format.lisp

(defgeneric copy-to (source filename)
  (:documentation
   "Load data from SOURCE and serialize it into FILENAME, using PostgreSQL
    COPY TEXT format."))

;; The next generic function is only to get instanciated for sources
;; actually containing more than a single source item (tables, collections,
;; etc)

(defgeneric copy-database (source
			   &key
			     truncate
			     data-only
			     schema-only
			     create-tables
			     include-drop
			     create-indexes
			     reset-sequences)
  (:documentation
   "Auto-discover source schema, convert it to PostgreSQL, migrate the data
    from the source definition to PostgreSQL for all the discovered
    items (tables, collections, etc), then reset the PostgreSQL sequences
    created by SERIAL columns in the first step.

    The target tables are automatically discovered, the only-tables
    parameter allows to filter them out."))


;;;
;;; Common API to introspec a data source, when that's possible. Typically
;;; when the source is a database system.
;;;

;; (defgeneric list-all-columns (connection &key)
;;   (:documentation "Discover all columns in CONNECTION source."))

;; (defgeneric list-all-indexes (connection &key)
;;   (:documentation "Discover all indexes in CONNECTION source."))

;; (defgeneric list-all-fkeys   (connection &key)
;;   (:documentation "Discover all foreign keys in CONNECTION source."))

;; (defgeneric fetch-metadata (connection &key)
;;   (:documentation "Full discovery of the CONNECTION data source."))


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

(defgeneric parse-header (md-copy header)
  (:documentation "Parse the file header and return a list of fields."))

(defgeneric process-rows (md-copy stream process-fn)
  (:documentation "Process rows from a given input stream."))



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
                              only-tables
                              create-indexes
                              foreign-keys
                              including
                              excluding))

(defgeneric prepare-pgsql-database (db-copy catalog
                                    &key
                                      materialize-views
                                      foreign-keys
                                      include-drop)
  (:documentation "Prepare the target PostgreSQL database."))

(defgeneric cleanup (db-copy catalog &key materialize-views)
  (:documentation "Clean-up after prepare-pgsql-database failure."))

(defgeneric complete-pgsql-database (db-copy catalog pkeys
                                     &key
                                       data-only
                                       foreign-keys
                                       reset-sequences)
  (:documentation "Alter load duties for database sources copy support."))

(defgeneric instanciate-table-copy-object (db-copy table)
  (:documentation "Create an new instance for copying TABLE data."))

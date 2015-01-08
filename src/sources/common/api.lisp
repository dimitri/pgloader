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

(defgeneric copy-to-queue (source queue)
  (:documentation
   "Load data from SOURCE and queue each row into QUEUE. Typicall
    implementation will directly use pgloader.queue:map-push-queue."))

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


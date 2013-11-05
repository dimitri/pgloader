;;;
;;; Generic API for pgloader sources
;;;
(in-package :pgloader.sources)

;; Abstract classes to define the API with
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
	      (loop for c in (slot-value source 'columns) collect nil))))))

(defgeneric map-rows (source &key process-row-fn)
  (:documentation
   "Read rows from source S and funcall PROCESS-ROW-FN for each of them."))

(defgeneric copy-to-queue (source dataq)
  (:documentation
   "Copy data as read in DATAQ into the source S target definition."))

(defgeneric copy-from (source &key truncate)
  (:documentation
   "Read data from source and concurrently stream it down to PostgreSQL"))

;; That one is more an export than a load. It always export to a single very
;; well defined format, the importing utility is defined in
;; src/pgsql-copy-format.lisp

(defgeneric copy-to (source filename)
  (:documentation
   "Extract data from source S into FILENAME, wth PostgreSQL COPY TEXT format."))

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
			     reset-sequences
			     only-tables)
  (:documentation
   "Auto-discover source schema, convert it to PostgreSQL, migrate the data
    from the source definition to PostgreSQL for all the discovered
    items (tables, collections, etc), then reset the PostgreSQL sequences
    created by SERIAL columns in the first step.

    The target tables are automatically discovered, the only-tables
    parameter allows to filter them out."))


;;;
;;; Filtering lists of columns and indexes
;;;
;;; A list of columns is expected to be an alist of table-name associated
;;; with a list of objects (clos or structures) that define the generic API
;;; described in src/pgsql/schema.lisp
;;;

(defun filter-column-list (all-columns &key only-tables including excluding)
  "Apply the filtering defined by the arguments:

    - keep only tables listed in ONLY-TABLES, or all of them if ONLY-TABLES
      is nil,

    - then unless EXCLUDING is nil, filter out the resulting list by
      applying the EXCLUDING regular expression list to table names in the
      all-columns list: we only keep the table names that match none of the
      regex in the EXCLUDING list

    - then unless INCLUDING is nil, only keep remaining elements that
      matches at least one of the INCLUDING regular expression list."

  (labels ((apply-filtering-rule (rule)
	     (declare (special table-name))
	     (typecase rule
	       (string (string-equal rule table-name))
	       (list   (destructuring-bind (type val) rule
			 (ecase type
			   (:regex (cl-ppcre:scan val table-name)))))))

	   (only (entry)
	     (let ((table-name (first entry)))
	       (or (null only-tables)
		   (member table-name only-tables :test #'equal))))

	   (exclude (entry)
	     (let ((table-name (first entry)))
	       (declare (special table-name))
	       (or (null excluding)
		   (notany #'apply-filtering-rule excluding))))

	   (include (entry)
	     (let ((table-name (first entry)))
	       (declare (special table-name))
	       (or (null including)
		   (some #'apply-filtering-rule including)))))

    (remove-if-not #'include
		   (remove-if-not #'exclude
				  (remove-if-not #'only all-columns)))))

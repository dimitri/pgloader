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
  (let ((transforms (when (slot-boundp source 'transforms)
		      (slot-value source 'transforms))))
    (unless transforms
      (setf (slot-value source 'transforms)
	    (loop for c in (slot-value source 'columns) collect nil)))))

(defgeneric map-rows (source &key process-row-fn)
  (:documentation
   "Read rows from source S and funcall PROCESS-ROW-FN for each of them."))

(defgeneric copy-to-queue (source dataq)
  (:documentation
   "Copy data as read in DATAQ into the source S target definition."))

(defgeneric copy-from (source &key)
  (:documentation
   "Read data from source and concurrently stream it down to PostgreSQL"))

;; That one is more an export than a load. It always export to a single very
;; well defined format, the importing utility is defined in
;; src/pgsql-copy-format.lisp

(defgeneric copy-to (source filename)
  (:documentation
   "Extract data from source S into FILENAME, wth PostgreSQL COPY TEXT format."))

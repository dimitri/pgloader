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

(defgeneric copy-to-queue (source queue)
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


;;;
;;; Some common tools for file based sources, such as CSV and FIXED
;;;
(defmacro with-open-file-or-stream ((&whole arguments
                                           stream filename-or-stream
                                           &key &allow-other-keys)
                                             &body body)
  "Generate a with-open-file call, or just bind STREAM varialbe to the
   FILENAME-OR-STREAM stream when this variable is of type STREAM."
  `(typecase ,filename-or-stream
     (stream (let ((,stream *standard-input*))
               ,@body))

     (t      (with-open-file (,stream ,filename-or-stream ,@(cddr arguments))
               ,@body))))

(defun get-pathname (dbname table-name &key (csv-path-root *csv-path-root*))
  "Return a pathname where to read or write the file data"
  (make-pathname
   :directory (pathname-directory
	       (merge-pathnames (format nil "~a/" dbname) csv-path-root))
   :name table-name
   :type "csv"))

(defun filter-directory (regex
			 &key
			   (keep :first) ; or :all
			   (root *csv-path-root*))
  "Walk the ROOT directory and KEEP either the :first or :all the matches
   against the given regexp."
  (let* ((candidates (pgloader.archive:get-matching-filenames root regex))
	 (candidates (ecase keep
		       (:first (list (first candidates)))
		       (:all   candidates))))
    (unless candidates
      (error "No file matching '~a' in expanded archive in '~a'" regex root))

    (loop for candidate in candidates
       do (if (probe-file candidate) candidate
	      (error "File does not exists: '~a'." candidate))
       finally (return candidates))))

(defun get-absolute-pathname (pathname-or-regex &key (root *csv-path-root*))
  "PATHNAME-OR-REGEX is expected to be either (:regexp expression)
   or (:filename pathname). In the first case, this fonction check if the
   pathname is absolute or relative and returns an absolute pathname given
   current working directory of ROOT.

   In the second case, walk the ROOT directory and return the first pathname
   that matches the regex. TODO: consider signaling a condition when we have
   more than one match."
  (destructuring-bind (type &rest part) pathname-or-regex
    (ecase type
      (:inline   (car part))		; because of &rest
      (:stdin    *standard-input*)
      (:regex    (destructuring-bind (keep regex) part
		   (filter-directory regex :keep keep :root root)))
      (:filename (let* ((filename (first part))
                        (realname
                         (if (fad:pathname-absolute-p filename) filename
                             (merge-pathnames filename root))))
		   (if (probe-file realname) realname
		       (error "File does not exists: '~a'." realname)))))))


;;;
;;; Project fields into columns
;;;
(defun project-fields (&key fields columns (compile t))
  "The simplest projection happens when both FIELDS and COLS are nil: in
   this case the projection is an identity, we simply return what we got.

   Other forms of projections consist of forming columns with the result of
   applying a transformation function. In that case a cols entry is a list
   of '(colname type expression), the expression being the (already
   compiled) function to use here."
  (labels ((null-as-processing-fn (null-as)
	     "return a lambda form that will process a value given NULL-AS."
	     (if (eq null-as :blanks)
		 (lambda (col)
		   (declare (optimize speed))
		   (if (every (lambda (char) (char= char #\Space)) col)
		       nil
		       col))
		 (lambda (col)
		   (declare (optimize speed))
		   (if (string= null-as col) nil col))))

	   (field-name-as-symbol (field-name-or-list)
	     "we need to deal with symbols as we generate code"
	     (typecase field-name-or-list
	       (list (pgloader.transforms:intern-symbol (car field-name-or-list)))
	       (t    (pgloader.transforms:intern-symbol field-name-or-list))))

	   (field-process-null-fn (field-name-or-list)
	     "Given a field entry, return a function dealing with nulls for it"
	     (destructuring-bind (&key null-as date-format &allow-other-keys)
		 (typecase field-name-or-list
		   (list (cdr field-name-or-list))
		   (t    (cdr (assoc field-name-or-list fields :test #'string=))))
	       (declare (ignore date-format)) ; TODO
	       (if (null null-as)
		   #'identity
		   (null-as-processing-fn null-as)))))

    (let* ((projection
	    (cond
	      ;; when no specific information has been given on FIELDS and
	      ;; COLUMNS, just apply generic NULL-AS processing
	      ((and (null fields) (null columns))
	       (lambda (row) (coerce row 'vector)))

	      ((null columns)
	       ;; when no specific information has been given on COLUMNS,
	       ;; use the information given for FIELDS and apply per-field
	       ;; null-as, or the generic one if none has been given for
	       ;; that field.
	       (let ((process-nulls
		      (mapcar (function field-process-null-fn) fields)))
		 `(lambda (row)
                    (let ((v (make-array (length row))))
                     (loop
                        :for i :from 0
                        :for col :in row
                        :for fn :in ',process-nulls
                        :do (setf (aref v i) (funcall fn col)))
                     v))))

	      (t
	       ;; project some number of FIELDS into a possibly different
	       ;; number of COLUMNS, using given transformation functions,
	       ;; processing NULL-AS represented values.
	       (let* ((args
		       (if fields
			   (mapcar (function field-name-as-symbol) fields)
			   (mapcar (function field-name-as-symbol) columns)))
		      (newrow
		       (loop for (name type fn) in columns
			  collect
			  ;; we expect the name of a COLUMN to be the same
			  ;; as the name of its derived FIELD when we
			  ;; don't have any transformation function
			    (or fn `(funcall ,(field-process-null-fn name)
					     ,(field-name-as-symbol name))))))
		 `(lambda (row)
		    (declare (optimize speed) (type list row))
		    (destructuring-bind (&optional ,@args &rest extra) row
		      (declare (ignorable ,@args) (ignore extra))
		      (vector ,@newrow))))))))
      ;; allow for some debugging
      (if compile (compile nil projection) projection))))

(defun reformat-then-process (&key fields columns target process-row-fn)
  "Return a lambda form to apply to each row we read.

   The lambda closes over the READ paramater, which is a counter of how many
   lines we did read in the file."
  (let ((projection (project-fields :fields fields :columns columns)))
    (lambda (row)
      (pgstate-incf *state* target :read 1)
      ;; cl-csv returns (nil) for an empty line
      (if (or (null row)
              (and (null (car row)) (null (cdr row))))
          (log-message :notice "Skipping empty line ~d."
                       (pgloader.utils::pgtable-read
                        (pgstate-get-table *state* target)))
          (let ((projected-vector
                 (handler-case
                     (funcall projection row)
                   (condition (e)
                     (pgstate-incf *state* target :errs 1)
                     (log-message :error "Could not read line ~d: ~a"
                                  (pgloader.utils::pgtable-read
                                   (pgstate-get-table *state* target))
                                  e)))))
            (when projected-vector
              (funcall process-row-fn projected-vector)))))))

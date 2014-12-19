;;;
;;; Generic API for pgloader sources
;;;
(in-package :pgloader.sources)

(define-condition connection-error (error)
  ((type :initarg :type :reader connection-error-type)
   (mesg :initarg :mesg :reader connection-error-mesg)
   (host :initarg :host :reader connection-error-host)
   (port :initarg :port :reader connection-error-port)
   (user :initarg :user :reader connection-error-user))
  (:report (lambda (err stream)
             (format stream "Failed to connect to ~a at ~s ~@[(port ~d)~]~@[ as user ~s: ~a~]"
                     (connection-error-type err)
                     (connection-error-host err)
                     (connection-error-port err)
                     (connection-error-user err)
                     (connection-error-mesg err)))))

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
		       (:first (when candidates (list (first candidates))))
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
      (:regex    (destructuring-bind (keep regex root) part
		   (filter-directory regex
                                     :keep keep
                                     :root (or *csv-path-root* root))))
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

	   (process-field (field-name-or-list)
	     "Given a field entry, return a function dealing with nulls for it"
	     (destructuring-bind (&key null-as
                                       date-format
                                       trim-both
                                       trim-left
                                       trim-right
                                       &allow-other-keys)
		 (typecase field-name-or-list
		   (list (cdr field-name-or-list))
		   (t    (cdr (assoc field-name-or-list fields
                                     :test #'string-equal))))
	       (declare (ignore date-format)) ; TODO
               ;; now prepare a function of a column
               (lambda (col)
                 (let ((value-or-null
                        (if (null null-as) col
                            (funcall (null-as-processing-fn null-as) col))))
                   (when value-or-null
                     (let ((value-or-null
                            (cond (trim-both
                                   (string-trim '(#\Space) value-or-null))
                                  (trim-left
                                   (string-left-trim '(#\Space) value-or-null))
                                  (trim-right
                                   (string-right-trim '(#\Space) value-or-null))
                                  (t          value-or-null))))
                       ;; now apply the date format, when given
                       (if date-format
                           (parse-date-string value-or-null
                                              (parse-date-format date-format))
                           value-or-null))))))))

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
		      (mapcar (function process-field) fields)))
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
                      (values
                       ;; make sure we apply fields level processing before
                       ;; we pass in the processed field values to the
                       ;; transformation functions, if any (null if blanks)
                       (loop for field-name in args
                          collect (list
                                   field-name
                                   `(funcall ,(process-field field-name)
                                             ,field-name))))
		      (newrow
		       (loop for (name type fn) in columns
			  collect
			  ;; we expect the name of a COLUMN to be the same
			  ;; as the name of its derived FIELD when we
			  ;; don't have any transformation function
			    (or fn `(funcall ,(process-field name)
					     ,(field-name-as-symbol name))))))
		 `(lambda (row)
		    (declare (optimize speed) (type list row))
		    (destructuring-bind (&optional ,@args &rest extra) row
		      (declare (ignorable ,@args) (ignore extra))
                      (let ,values
                        (declare (ignorable ,@args))
                        (vector ,@newrow)))))))))
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


;;;
;;; Type casting machinery, to share among all database kind sources.
;;;

;;
;; The special variables *default-cast-rules* and *cast-rules* must be bound
;; by specific database commands with proper values at run-time.
;;
(defvar *default-cast-rules* nil "Default casting rules.")
(defvar *cast-rules* nil "Specific casting rules added in the command.")

;;;
;;; Handling typmod in the general case, don't apply to ENUM types
;;;
(defun parse-column-typemod (data-type column-type)
  "Given int(7), returns the number 7.

   Beware that some data-type are using a typmod looking definition for
   things that are not typmods at all: enum."
  (unless (or (string= "enum" data-type)
	      (string= "set" data-type))
    (let ((start-1 (position #\( column-type))	; just before start position
	  (end     (position #\) column-type)))	; just before end position
      (when start-1
	(destructuring-bind (a &optional b)
	    (mapcar #'parse-integer
		    (sq:split-sequence #\, column-type
				       :start (+ 1 start-1) :end end))
	  (cons a b))))))

(defun typemod-expr-to-function (expr)
  "Transform given EXPR into a callable function object."
  `(lambda (typemod)
     (destructuring-bind (precision &optional (scale 0)) typemod
       (declare (ignorable precision scale))
       ,expr)))

(defun typemod-expr-matches-p (rule-typemod-expr typemod)
  "Check if an expression such as (< 10) matches given typemod."
  (funcall (compile nil (typemod-expr-to-function rule-typemod-expr)) typemod))

(defun cast-rule-matches (rule source)
  "Returns the target datatype if the RULE matches the SOURCE, or nil"
  (destructuring-bind (&key ((:source rule-source))
			    ((:target rule-target))
			    using)
      rule
    (destructuring-bind
	  ;; it's either :type or :column, just cope with both thanks to
	  ;; &allow-other-keys
	  (&key ((:type rule-source-type) nil t-s-p)
		((:column rule-source-column) nil c-s-p)
		((:typemod typemod-expr) nil tm-s-p)
		((:default rule-source-default) nil d-s-p)
		((:not-null rule-source-not-null) nil n-s-p)
		((:auto-increment rule-source-auto-increment) nil ai-s-p)
		&allow-other-keys)
	rule-source
      (destructuring-bind (&key table-name
				column-name
				type
				ctype
				typemod
				default
				not-null
				auto-increment)
	  source
	(declare (ignore ctype))
	(when
	    (and
	     (or (and t-s-p (string= type rule-source-type))
		 (and c-s-p
		      (string-equal table-name (car rule-source-column))
		      (string-equal column-name (cdr rule-source-column))))
	     (or (null tm-s-p) (typemod-expr-matches-p typemod-expr typemod))
	     (or (null d-s-p)  (string= default rule-source-default))
	     (or (null n-s-p)  (eq not-null rule-source-not-null))
	     (or (null ai-s-p) (eq auto-increment rule-source-auto-increment)))
	  (list :using using :target rule-target))))))

(defun format-pgsql-default-value (default &optional using-cast-fn)
  "Returns suitably quoted default value for CREATE TABLE command."
  (cond
    ((null default) "NULL")
    ((and (stringp default) (string= "NULL" default)) default)
    ((and (stringp default) (string= "CURRENT_TIMESTAMP" default)) default)
    (t
     ;; apply the transformation function to the default value
     (if using-cast-fn (format-pgsql-default-value
			(funcall using-cast-fn default))
	 (format nil "'~a'" default)))))

(defun format-pgsql-type (source target using)
  "Returns a string suitable for a PostgreSQL type definition"
  (destructuring-bind (&key ((:table-name source-table-name))
			    ((:column-name source-column-name))
			    ((:type source-type))
			    ((:ctype source-ctype))
			    ((:typemod source-typemod))
			    ((:default source-default))
			    ((:not-null source-not-null))
			    &allow-other-keys)
      source
    (if target
	(destructuring-bind (&key type
				  drop-default
				  drop-not-null
				  (drop-typemod t)
				  &allow-other-keys)
	    target
	  (let ((type-name
		 (typecase type
		   (function (funcall type
				      source-table-name source-column-name
				      source-type source-ctype source-typemod))
		   (t type)))
		(pg-typemod
		 (when source-typemod
		   (destructuring-bind (a . b) source-typemod
		     (format nil "(~a~:[~*~;,~a~])" a b b)))))
	    (format nil
		    "~a~:[~*~;~a~]~:[~; not null~]~:[~; default ~a~]"
		    type-name
		    (and source-typemod (not drop-typemod))
		    pg-typemod
		    (and source-not-null (not drop-not-null))
		    (and source-default (not drop-default))
		    (format-pgsql-default-value source-default using))))

	;; NO MATCH
	;;
	;; prefer char(24) over just char, that is the column type over the
	;; data type.
        (format nil "~a~:[~; not null~]~:[~; default ~a~]"
                source-ctype
                source-not-null
                source-default
                (format-pgsql-default-value source-default using)))))

(defun apply-casting-rules (dtype ctype default nullable extra
			    &key
			      table-name column-name ; ENUM support
			      (rules (append *cast-rules*
					     *default-cast-rules*)))
  "Apply the given RULES to the MySQL SOURCE type definition"
  (let* ((typemod        (parse-column-typemod dtype ctype))
	 (not-null       (string-equal nullable "NO"))
	 (auto-increment (string= "auto_increment" extra))
	 (source        `(:table-name ,table-name
                                      :column-name ,column-name
                                      :type ,dtype
                                      :ctype ,ctype
                                      ,@(when typemod (list :typemod typemod))
                                      :default ,default
                                      :not-null ,not-null
                                      :auto-increment ,auto-increment)))
    (let (first-match-using)
      (loop
         for rule in rules
         for (target using) = (destructuring-bind (&key target using)
                                  (cast-rule-matches rule source)
                                (list target using))
         do (when (and (null target) using (null first-match-using))
              (setf first-match-using using))
         until target
         finally
           (return
             (list :transform-fn (or first-match-using using)
                   :pgtype (format-pgsql-type source target using)))))))

(defun cast (table-name column-name dtype ctype default nullable extra)
  "Convert a MySQL datatype to a PostgreSQL datatype.

DYTPE is the MySQL data_type and CTYPE the MySQL column_type, for example
that would be int and int(7) or varchar and varchar(25)."
  (destructuring-bind (&key pgtype transform-fn &allow-other-keys)
      (apply-casting-rules dtype ctype default nullable extra
			   :table-name table-name
			   :column-name column-name)
    (values pgtype transform-fn)))

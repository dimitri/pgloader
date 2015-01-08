;;;
;;; Type casting machinery, to share among all database kind sources.
;;;
(in-package #:pgloader.sources)

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

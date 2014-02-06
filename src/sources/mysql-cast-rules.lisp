;;;
;;; Tools to handle MySQL data type casting rules
;;;

(in-package :pgloader.mysql)

;;;
;;; Some functions to deal with ENUM types
;;;
(defun explode-mysql-enum (ctype)
  "Convert MySQL ENUM expression into a list of labels."
  ;; from: "ENUM('small', 'medium', 'large')"
  ;;   to: ("small" "medium" "large")
  (mapcar (lambda (x) (string-trim "' )" x))
	  (sq:split-sequence #\, ctype  :start (position #\' ctype))))

(defun get-enum-type-name (table-name column-name identifier-case)
  "Return the Type Name we're going to use in PostgreSQL."
  (apply-identifier-case (format nil "~a_~a" table-name column-name)
			 identifier-case))

(defun get-create-enum (table-name column-name ctype
			&key (identifier-case :downcase))
  "Return a PostgreSQL CREATE ENUM TYPE statement from MySQL enum column."
  (with-output-to-string (s)
    (format s "CREATE TYPE ~a AS ENUM (~{'~a'~^, ~});"
	    (get-enum-type-name table-name column-name identifier-case)
	    (explode-mysql-enum ctype))))

(defun cast-enum (table-name column-name type ctype typemod)
  "Cast MySQL inline ENUM type to using a PostgreSQL named type.

   The type naming is hardcoded to be table-name_column-name"
  (declare (ignore type ctype typemod))
  (format nil "\"~a_~a\"" table-name column-name))

(defun cast-set (table-name column-name type ctype typemod)
  "Cast MySQL inline SET type to using a PostgreSQL ENUM Array.

   The ENUM data type name is hardcoded to be table-name_column-name"
  (declare (ignore type ctype typemod))
  (format nil "\"~a_~a\"[]" table-name column-name))

;;;
;;; The default MySQL Type Casting Rules
;;;
(defparameter *default-cast-rules*
  `((:source (:type "int" :auto-increment t :typemod (< precision 10))
     :target (:type "serial"))

    (:source (:type "int" :auto-increment t :typemod (<= 10 precision))
     :target (:type "bigserial"))

    (:source (:type "int" :auto-increment nil :typemod (< precision 10))
     :target (:type "int"))

    (:source (:type "int" :auto-increment nil :typemod (<= 10 precision))
     :target (:type "bigint"))

    ;; bigint and smallint with auto_increment always are [big]serial
    (:source (:type "smallint" :auto-increment t) :target (:type "serial"))
    (:source (:type "bigint" :auto-increment t) :target (:type "bigserial"))

    ;; actually tinyint(1) is most often used as a boolean
    (:source (:type "tinyint" :typemod (= 1 precision))
	     :target (:type "boolean" :drop-typemod t)
	     :using pgloader.transforms::tinyint-to-boolean)

    ;; we need the following to benefit from :drop-typemod
    (:source (:type "tinyint")   :target (:type "smallint" :drop-typemod t))
    (:source (:type "smallint")  :target (:type "smallint" :drop-typemod t))
    (:source (:type "mediumint") :target (:type "integer"  :drop-typemod t))
    (:source (:type "integer")   :target (:type "integer"  :drop-typemod t))
    (:source (:type "float")     :target (:type "float"    :drop-typemod t))
    (:source (:type "bigint")    :target (:type "bigint"   :drop-typemod t))

    (:source (:type "double")
	     :target (:type "double precision" :drop-typemod t))

    (:source (:type "numeric")
     :target (:type "numeric" :drop-typemod nil))

    (:source (:type "decimal")
     :target (:type "decimal" :drop-typemod nil))

    ;; the text based types
    (:source (:type "varchar")    :target (:type "text"))
    (:source (:type "tinytext")   :target (:type "text"))
    (:source (:type "text")       :target (:type "text"))
    (:source (:type "mediumtext") :target (:type "text"))
    (:source (:type "longtext")   :target (:type "text"))

    (:source (:type "char")
     :target (:type "varchar" :drop-typemod nil))

    ;;
    ;; cl-mysql returns binary values as a simple-array of bytes (as in
    ;; ‘(UNSIGNED-BYTE 8)), that we then need to represent as proper
    ;; PostgreSQL bytea input.
    ;;
    (:source (:type "binary") :target (:type "bytea")
	     :using pgloader.transforms::byte-vector-to-bytea)
    (:source (:type "varbinary")  :target (:type "bytea")
	     :using pgloader.transforms::byte-vector-to-bytea)
    (:source (:type "tinyblob")   :target (:type "bytea")
	     :using pgloader.transforms::byte-vector-to-bytea)
    (:source (:type "blob")       :target (:type "bytea")
	     :using pgloader.transforms::byte-vector-to-bytea)
    (:source (:type "mediumblob") :target (:type "bytea")
	     :using pgloader.transforms::byte-vector-to-bytea)
    (:source (:type "longblob")   :target (:type "bytea")
	     :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "datetime" :default "0000-00-00 00:00:00" :not-null t)
     :target (:type "timestamptz" :drop-default t :drop-not-null t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "datetime" :default "0000-00-00 00:00:00")
     :target (:type "timestamptz" :drop-default t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "timestamp" :default "0000-00-00 00:00:00" :not-null t)
     :target (:type "timestamptz" :drop-default t :drop-not-null t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "timestamp" :default "0000-00-00 00:00:00")
     :target (:type "timestamptz" :drop-default t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "date" :default "0000-00-00")
     :target (:type "date" :drop-default t)
     :using pgloader.transforms::zero-dates-to-null)

    ;; date types without strange defaults
    (:source (:type "date")      :target (:type "date"))
    (:source (:type "datetime")  :target (:type "timestamptz"))
    (:source (:type "timestamp") :target (:type "timestamptz"))
    (:source (:type "year")      :target (:type "integer" :drop-typemod t))

    ;; Inline MySQL "interesting" datatype
    (:source (:type "enum")
     :target (:type ,#'cast-enum))

    (:source (:type "set")
     :target (:type ,#'cast-set)
     :using pgloader.transforms::set-to-enum-array)

    ;; geometric data types, just POINT for now
    (:source (:type "point")
     :target (:type "point")
     :using pgloader.transforms::convert-mysql-point))
  "Data Type Casting rules to migrate from MySQL to PostgreSQL")

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
       ;;
       ;; The command parser interns symbols into the pgloader.transforms
       ;; package, whereas the default casting rules are defined in the
       ;; pgloader.mysql package. Have a compatibility layer here for the
       ;; generated code.
       ;;
       (let ((pgloader.transforms::precision precision))
	 (declare (ignorable pgloader.transforms::precision))
	 ,expr))))

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
    ((string= "NULL" default) default)
    ((string= "CURRENT_TIMESTAMP" default) default)
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
	source-ctype)))

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

(defun list-transforms (columns)
  "Return the list of transformation functions to apply to a given table."
  (loop
     for col in columns
     collect (with-slots (name dtype ctype default nullable extra) col
	       (get-transform-function dtype ctype default nullable extra))))

(defun test-casts ()
  "Just test some cases for the casts"
  (let ((*cast-rules*
	 '(;; (:source (:column "col1" :auto-increment nil)
	   ;;  :target (:type "timestamptz"
	   ;; 	     :drop-default nil
	   ;; 	     :drop-not-null nil)
	   ;;  :using nil)

	   (:source (:type "varchar" :auto-increment nil)
	    :target (:type "text" :drop-default nil :drop-not-null nil)
	    :using nil)

	   (:source (:type "char" :typemod (= (car typemod) 1))
	    :target (:type "char" :drop-typemod nil))

           (:source (:column ("table" . "g"))
            :target nil
            :using pgloader.transforms::empty-string-to-null)))

	(columns
	 ;; name dtype       ctype         default nullable extra
	 '(("a"  "int"       "int(7)"      nil "NO" "auto_increment")
	   ("b"  "int"       "int(10)"     nil "NO" "auto_increment")
	   ("c"  "varchar"   "varchar(25)" nil  nil nil)
	   ("d"  "tinyint"   "tinyint(4)"  "0" nil nil)
	   ("e"  "datetime"  "datetime"    "0000-00-00 00:00:00" nil nil)
	   ("f"  "date"      "date"        "0000-00-00" "NO" nil)
	   ("g"  "enum"      "ENUM('a', 'b')"  nil nil nil)
	   ("h"  "set"       "SET('a', 'b')"   nil nil nil)
	   ("i"  "int"       "int(11)"         nil nil nil)
	   ("j"  "float"     "float(12,2)"     nil nil nil)
	   ("k"  "double"    "double unsigned" nil nil nil)
	   ("l"  "bigint"    "bigint(20)"      nil nil nil)
	   ("m"  "numeric"   "numeric(18,3)"   nil nil nil)
	   ("n"  "decimal"   "decimal(15,5)"   nil nil nil)
	   ("o"  "timestamp" "timestamp" "CURRENT_TIMESTAMP" "NO" "on update CURRENT_TIMESTAMP")
	   ("p"  "point"     "point"     nil "YES" nil)
	   ("q"  "char"      "char(5)"   nil "YES" nil)
	   ("l"  "char"      "char(1)"   nil "YES" nil)
	   ("m"  "integer"   "integer(4)"    nil "YES" nil)
	   ("o"  "tinyint"   "tinyint(1)" "0" nil nil)
           ("p"  "smallint"  "smallint(5) unsigned" nil "no" "auto_increment"))))

    ;;
    ;; format-pgsql-column when given a mysql-column would call `cast' for
    ;; us, but here we want more control over the ouput, so we call it
    ;; ourselves.
    ;;
    (format t "   ~a~30T~a~65T~a~%" "MySQL ctype" "PostgreSQL type" "transform")
    (format t "   ~a~30T~a~65T~a~%" "-----------" "---------------" "---------")
    (loop
       for (name dtype ctype nullable default extra) in columns
       for mycol = (make-mysql-column "table" name dtype ctype nullable default extra)
       for (pgtype fn) = (multiple-value-bind (pgcol fn)
                             (cast "table" name dtype ctype nullable default extra)
                           (list pgcol fn))
       do
	 (format t "~a: ~a~30T~a~65T~:[~;using ~a~]~%" name ctype pgtype fn fn))))

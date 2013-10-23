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
  (format nil "~a_~a" table-name column-name))

;;;
;;; The default MySQL Type Casting Rules
;;;
(defparameter *default-cast-rules*
  `((:source (:type "int" :auto-increment t :typemod (< (car typemod) 10))
     :target (:type "serial"))

    (:source (:type "int" :auto-increment t :typemod (<= 10 (car typemod)))
     :target (:type "bigserial"))

    (:source (:type "int" :auto-increment nil :typemod (< (car typemod) 10))
     :target (:type "int"))

    (:source (:type "int" :auto-increment nil :typemod (<= 10 (car typemod)))
     :target (:type "bigint"))

    ;; bigint with auto_increment always are bigserial
    (:source (:type "bigint" :auto-increment t) :target (:type "bigserial"))

    ;; we need the following to benefit from :drop-typemod
    (:source (:type "tinyint")   :target (:type "smallint"))
    (:source (:type "smallint")  :target (:type "smallint"))
    (:source (:type "mediumint") :target (:type "integer"))
    (:source (:type "float")     :target (:type "float"))
    (:source (:type "bigint")    :target (:type "bigint"))
    (:source (:type "double")    :target (:type "double precision"))

    (:source (:type "numeric")
     :target (:type "numeric" :drop-typemod nil))

    (:source (:type "decimal")
     :target (:type "decimal" :drop-typemod nil))

    (:source (:type "varchar")    :target (:type "text"))
    (:source (:type "tinytext")   :target (:type "text"))
    (:source (:type "text")       :target (:type "text"))
    (:source (:type "mediumtext") :target (:type "text"))
    (:source (:type "longtext")   :target (:type "text"))

    ;;
    ;; cl-mysql and postmodern are adapting binary values as a simple-array
    ;; (or vector) of ‘(UNSIGNED-BYTE 8), so there should be no other
    ;; explicit conversion to do here.
    ;;
    (:source (:type "binary")     :target (:type "bytea"))
    (:source (:type "varbinary")  :target (:type "bytea"))
    (:source (:type "tinyblob")   :target (:type "bytea"))
    (:source (:type "blob")       :target (:type "bytea"))
    (:source (:type "mediumblob") :target (:type "bytea"))
    (:source (:type "longblob")   :target (:type "bytea"))

    (:source (:type "datetime" :default "0000-00-00 00:00:00" :not-null t)
     :target (:type "timestamptz" :drop-default t :drop-not-null t))

    (:source (:type "datetime" :default "0000-00-00 00:00:00")
     :target (:type "timestamptz" :drop-default t))

    (:source (:type "timestamp" :default "0000-00-00 00:00:00" :not-null t)
     :target (:type "timestamptz" :drop-default t :drop-not-null t))

    (:source (:type "timestamp" :default "0000-00-00 00:00:00")
     :target (:type "timestamptz" :drop-default t))

    (:source (:type "date" :default "0000-00-00")
     :target (:type "date" :drop-default t))

    ;; date types without strange defaults
    (:source (:type "date")      :target (:type "date"))
    (:source (:type "datetime")  :target (:type "timestamptz"))
    (:source (:type "timestamp") :target (:type "timestamptz"))
    (:source (:type "year")      :target (:type "integer"))

    (:source (:type "enum")
     :target (:type ,#'cast-enum))

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
  (unless (string= "enum" data-type)
    (let ((start-1 (position #\( column-type))	; just before start position
	  (end     (position #\) column-type)))	; just before end position
      (when start-1
	(destructuring-bind (a &optional b)
	    (mapcar #'parse-integer
		    (sq:split-sequence #\, column-type
				       :start (+ 1 start-1) :end end))
	  (cons a b))))))

(defun typemod-expr-matches-p (rule-typemod-expr typemod)
  "Check if an expression such as (< 10) matches given typemod."
  (funcall (compile nil `(lambda (typemod) ,rule-typemod-expr)) typemod))

(defun cast-rule-matches (rule source)
  "Returns the target datatype if the RULE matches the SOURCE, or nil"
  (destructuring-bind (&key ((:source rule-source))
			    ((:target rule-target))
			    using)
      rule
    (destructuring-bind
	  (&key ((:type rule-source-type) nil t-s-p)
		((:typemod typemod-expr) nil tm-s-p)
		((:default rule-source-default) nil d-s-p)
		((:not-null rule-source-not-null) nil n-s-p)
		((:auto-increment rule-source-auto-increment) nil ai-s-p))
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
	(declare (ignore table-name column-name ctype))
	(when
	    (and
	     (string= type rule-source-type)
	     (or (null tm-s-p) (typemod-expr-matches-p typemod-expr typemod))
	     (or (null d-s-p)  (string= default rule-source-default))
	     (or (null n-s-p)  (eq not-null rule-source-not-null))
	     (or (null ai-s-p) (eq auto-increment rule-source-auto-increment)))
	  (list :using using :target rule-target))))))

(defun format-pgsql-default-value (default using-cast-fn)
  "Returns suitably quoted default value for CREATE TABLE command."
  (cond
    ((string= "NULL" default) default)
    ((string= "CURRENT_TIMESTAMP" default) default)
    (t
     (format nil "'~a'"
	     ;; apply the transformation function to the default value
	     (if using-cast-fn (funcall using-cast-fn default) default)))))

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
	 (source         (append (list :table-name table-name)
				 (list :column-name column-name)
				 (list :type dtype)
				 (list :ctype ctype)
				 (when typemod (list :typemod typemod))
				 (list :default default)
				 (list :not-null not-null)
				 (list :auto-increment auto-increment))))
   (loop
      for rule in rules
      for target? = (cast-rule-matches rule source)
      until target?
      finally
	(return
	  (destructuring-bind (&key target using &allow-other-keys)
	      target?
	    (list :transform-fn using
		  :pgtype (format-pgsql-type source target using)))))))

(defun get-transform-function (dtype ctype default nullable extra)
  "Apply given RULES and return the tranform function needed for this column"
  (destructuring-bind (&key transform-fn &allow-other-keys)
      (apply-casting-rules dtype ctype default nullable extra)
    transform-fn))

(defun cast (table-name column-name dtype ctype default nullable extra)
  "Convert a MySQL datatype to a PostgreSQL datatype.

DYTPE is the MySQL data_type and CTYPE the MySQL column_type, for example
that would be int and int(7) or varchar and varchar(25)."
  (destructuring-bind (&key pgtype &allow-other-keys)
      (apply-casting-rules dtype ctype default nullable extra
			   :table-name table-name
			   :column-name column-name)
    pgtype))

(defun transforms (columns)
  "Return the list of transformation functions to apply to a given table."
  (loop
     for (name dtype ctype default nullable extra) in columns
     collect (get-transform-function dtype ctype default nullable extra)))

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

	   (:source (:type "tinyint" :auto-increment nil)
	    :target (:type "boolean" :drop-default nil :drop-not-null nil)
	    :using pgloader.transforms::tinyint-to-boolean)

	   (:source (:type "datetime" :auto-increment nil)
	    :target (:type "timestamptz" :drop-default t :drop-not-null t)
	    :using pgloader.transforms::zero-dates-to-null)

	   (:source (:type "timestamp" :auto-increment nil)
	    :target (:type "timestamptz" :drop-default nil :drop-not-null t)
	    :using pgloader.transforms::zero-dates-to-null)

	   (:source (:type "date" :auto-increment nil)
	    :target (:type "date" :drop-default t :drop-not-null t)
	    :using pgloader.transforms::zero-dates-to-null)))

	(columns
	 ;; name dtype       ctype         default nullable extra
	 '(("a"  "int"       "int(7)"      nil "NO" "auto_increment")
	   ("b"  "int"       "int(10)"     nil "NO" "auto_increment")
	   ("c"  "varchar"   "varchar(25)" nil  nil nil)
	   ("d"  "tinyint"   "tinyint(4)"  "0" nil nil)
	   ("e"  "datetime"  "datetime"    "0000-00-00 00:00:00" nil nil)
	   ("f"  "date"      "date"        "0000-00-00" "NO" nil)
	   ("g"  "enum"      "ENUM('a', 'b')"  nil nil nil)
	   ("h"  "int"       "int(11)"         nil nil nil)
	   ("i"  "float"     "float(12,2)"     nil nil nil)
	   ("j"  "double"    "double unsigned" nil nil nil)
	   ("k"  "bigint"    "bigint(20)"      nil nil nil)
	   ("l"  "numeric"   "numeric(18,3)"   nil nil nil)
	   ("m"  "decimal"   "decimal(15,5)"   nil nil nil)
	   ("n"  "timestamp" "timestamp" "CURRENT_TIMESTAMP" "NO" "on update CURRENT_TIMESTAMP")
	   ("o"  "point"     "point"     nil "YES" nil))))

    (loop
       for (name dtype ctype nullable default extra) in columns
       for pgtype = (cast "table" name dtype ctype nullable default extra)
       for fn in (transforms columns)
       do
	 (format t "~a: ~a~20T~a~45T~:[~;using ~a~]~%" name ctype pgtype fn fn))))

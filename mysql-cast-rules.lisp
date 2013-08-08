;;;
;;; Tools to handle MySQL data type casting rules
;;;

(in-package :pgloader.mysql)

(defparameter *default-cast-rules*
  '((:source (:type "int" :auto-increment t :typemod (< 10))
     :target (:type "serial"))

    (:source (:type "int" :auto-increment t :typemod (>= 10))
     :target (:type "bigserial"))

    (:source (:type "int" :auto-increment nil :typemod (< 10))
     :target (:type "int"))

    (:source (:type "int" :auto-increment nil :typemod (>= 10))
     :target (:type "bigint"))

    (:source (:type "varchar")  :target (:type "text"))

    (:source (:type "datetime" :default "0000-00-00 00:00:00" :not-null t)
     :target (:type "timestamptz" :drop-default t :drop-not-null t))

    (:source (:type "datetime" :default "0000-00-00 00:00:00")
     :target (:type "timestamptz" :drop-default t))

    (:source (:type "date" :default "0000-00-00")
     :target (:type "date" :drop-default t))

    (:source (:type "datetime") :target (:type "timestamptz")))
  "Data Type Casting rules to migrate from MySQL to PostgreSQL")

(defvar *cast-rules* nil "Specific casting rules added in the command.")

(defun parse-column-typemod (column-type)
  "Given int(7), returns the number 7."
  (let ((splits (sq:split-sequence-if (lambda (c) (member c '(#\( #\))))
				      column-type
				      :remove-empty-subseqs t)))
    (if (= 1 (length splits))
	nil
     (parse-integer (nth 1 splits)))))

(defun typemod-expr-matches-p (rule-typemod-expr typemod)
  "Check if an expression such as (< 10) matches given typemod."
  (destructuring-bind (op threshold) rule-typemod-expr
    (funcall (symbol-function op) typemod threshold)))

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
      (destructuring-bind (&key type
				ctype
				typemod
				default
				not-null
				auto-increment)
	  source
	(declare (ignore ctype))
	(when
	    (and
	     (string= type rule-source-type)
	     (or (null tm-s-p) (typemod-expr-matches-p typemod-expr typemod))
	     (or (null d-s-p)  (string= default rule-source-default))
	     (or (null n-s-p)  (eq not-null rule-source-not-null))
	     (or (null ai-s-p) (eq auto-increment rule-source-auto-increment)))
	  (list :using using :target rule-target))))))

(defun format-pgsql-type (source target using)
  "Returns a string suitable for a PostgreSQL type definition"
  (destructuring-bind (&key ((:type source-type))
			    ctype
			    default
			    not-null
			    &allow-other-keys)
      source
    (if target
	(destructuring-bind (&key type drop-default drop-not-null
				  &allow-other-keys)
	    target
	  (format nil
		  "~a~:[~; not null~]~:[~; default '~a'~]"
		  type
		  (and not-null (not drop-not-null))
		  (and default (not drop-default))
		  ;; apply the transformation function to the default value
		  (if using (funcall using default) default)))

	;; NO MATCH
	;;
	;; prefer char(24) over just char, that is the column type over the
	;; data type.
	ctype)))

(defun apply-casting-rules (dtype ctype default nullable extra
			    &optional (rules (append *cast-rules*
						     *default-cast-rules*)))
  "Apply the given RULES to the MySQL SOURCE type definition"
  (let* ((typemod        (parse-column-typemod ctype))
	 (not-null       (string-equal nullable "NO"))
	 (auto-increment (string= "auto_increment" extra))
	 (source         (append (list :type dtype)
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

(defun cast (dtype ctype default nullable extra)
  "Convert a MySQL datatype to a PostgreSQL datatype.

DYTPE is the MySQL data_type and CTYPE the MySQL column_type, for example
that would be int and int(7) or varchar and varchar(25)."
  (destructuring-bind (&key pgtype &allow-other-keys)
      (apply-casting-rules dtype ctype default nullable extra)
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

	   (:source (:type "int" :auto-increment t)
	    :target (:type "bigserial" :drop-default nil :drop-not-null nil)
	    :using nil)

	   (:source (:type "tinyint" :auto-increment nil)
	    :target (:type "boolean" :drop-default nil :drop-not-null nil)
	    :using pgloader.transforms::tinyint-to-boolean)

	   (:source (:type "datetime" :auto-increment nil)
	    :target (:type "timestamptz" :drop-default t :drop-not-null t)
	    :using pgloader.transforms::zero-dates-to-null)

	   (:source (:type "date" :auto-increment nil)
	    :target (:type "date" :drop-default t :drop-not-null t)
	    :using pgloader.transforms::zero-dates-to-null)))

	(columns
	 ;; name dtype      ctype         default nullable extra
	 '(("a"  "int"      "int(7)"      nil "NO" "auto_increment")
	   ("b"  "int"      "int(10)"     nil "NO" "auto_increment")
	   ("c"  "varchar"  "varchar(25)" nil  nil nil)
	   ("d"  "tinyint"  "tinyint(4)"  "0" nil nil)
	   ("e"  "datetime" "datetime"    "0000-00-00 00:00:00" nil nil)
	   ("f"  "date"     "date"        "0000-00-00" "NO" nil))))

    (loop
       for (name dtype ctype nullable default extra) in columns
       for pgtype = (cast dtype ctype nullable default extra)
       for fn in (transforms columns)
       do
	 (format t "~a~14T~a~45T~:[~;using ~a~]~%" ctype pgtype fn fn))))

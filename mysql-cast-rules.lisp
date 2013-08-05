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
				typemod
				default
				not-null
				auto-increment)
	  source
	(when
	    (and
	     (string= type rule-source-type)
	     (or (null tm-s-p) (typemod-expr-matches-p typemod-expr typemod))
	     (or (null d-s-p)  (string= default rule-source-default))
	     (or (null n-s-p)  (eq not-null rule-source-not-null))
	     (or (null ai-s-p) (eq auto-increment rule-source-auto-increment)))
	  rule-target)))))

(defun format-pgsql-type (source target)
  "Returns a string suitable for a PostgreSQL type definition"
  (destructuring-bind (&key ((:type source-type))
			    default not-null &allow-other-keys)
      source
    (if target
	(destructuring-bind (&key type drop-default drop-not-null
				  &allow-other-keys)
	    target
	  (format nil
		  "~a~:[ not null~;~]~:[~; default ~a~]"
		  type
		  (and (not not-null) drop-not-null)
		  (and default (not drop-default)) default))
	source-type)))

(defun apply-casting-rules (source
			    &optional (rules (append *cast-rules*
						     *default-cast-rules*)))
  "Apply the given RULES to the MySQL SOURCE type definition"
  (loop
     for rule in rules
     for target = (cast-rule-matches rule source)
     until target
     finally (return (format-pgsql-type source target))))

(defun cast (dtype ctype nullable default extra)
  "Convert a MySQL datatype to a PostgreSQL datatype.

DYTPE is the MySQL data_type and CTYPE the MySQL column_type, for example
that would be int and int(7) or varchar and varchar(25)."
  (let* ((typemod        (parse-column-typemod ctype))
	 (not-null       (string= nullable "NO"))
	 (auto-increment (string= "auto_increment" extra)))
    (apply-casting-rules (append (list :type dtype)
				 (when typemod (list :typemod typemod))
				 (list :default default)
				 (list :not-null not-null)
				 (list :auto-increment auto-increment)))))

(defun test-casts ()
  "Just test some cases for the casts"
  (let ((*cast-rules*
	 '(;; (:SOURCE (:COLUMN "col1" :AUTO-INCREMENT NIL)
	   ;;  :TARGET (:TYPE "timestamptz"
	   ;; 	     :DROP-DEFAULT NIL
	   ;; 	     :DROP-NOT-NULL NIL)
	   ;;  :USING NIL)

	   (:SOURCE (:TYPE "varchar" :AUTO-INCREMENT NIL)
	    :TARGET (:TYPE "text" :DROP-DEFAULT NIL :DROP-NOT-NULL NIL)
	    :USING NIL)

	   (:SOURCE (:TYPE "int" :AUTO-INCREMENT T)
	    :TARGET (:TYPE "bigserial" :DROP-DEFAULT NIL :DROP-NOT-NULL NIL)
	    :USING NIL)

	   (:SOURCE (:TYPE "datetime" :AUTO-INCREMENT NIL)
	    :TARGET (:TYPE "timestamptz" :DROP-DEFAULT t :DROP-NOT-NULL t)
	    :USING NIL)

	   (:SOURCE (:TYPE "date" :AUTO-INCREMENT NIL)
	    :TARGET (:TYPE "date" :DROP-DEFAULT T :DROP-NOT-NULL T)
	    :USING PGLOADER.TRANSFORMS::ZERO-DATES-TO-NULL))))
    (loop
       for (dtype ctype nullable default extra)
       in '(("int" "int(7)" nil nil "auto_increment")
	    ("int" "int(10)" nil nil "auto_increment")
	    ("varchar" "varchar(25)" t nil nil)
	    ("datetime" "datetime" nil "0000-00-00 00:00:00" nil)
	    ("date" "date" nil "0000-00-00" nil))
       do
	 (format t "~a~14T~a~%"
		 ctype
		 (cast dtype ctype nullable default extra)))))

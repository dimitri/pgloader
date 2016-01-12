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
	  (list a b))))))

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

(defun make-pgsql-type (source target using)
  "Returns a COLUMN struct suitable for a PostgreSQL type definition"
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
		   (destructuring-bind (a &optional b) source-typemod
		     (format nil "(~a~:[~*~;,~a~])" a b b)))))
            (make-column :name (apply-identifier-case source-column-name)
                         :type-name type-name
                         :type-mod (when (and source-typemod (not drop-typemod))
                                     pg-typemod)
                         :nullable (not (and source-not-null (not drop-not-null)))
                         :default (when (and source-default (not drop-default))
                                    (format-default-value source-default using))
                         :transform using)))

	;; NO MATCH
	;;
	;; prefer char(24) over just char, that is the column type over the
	;; data type.
        (make-column :name (apply-identifier-case source-column-name)
                     :type-name source-ctype
                     :nullable (not source-not-null)
                     :default (format-default-value source-default using)
                     :transform using))))

(defun apply-casting-rules (table-name column-name
                            dtype ctype default nullable extra
                            &key
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
         :for rule :in rules
         :for (target using) := (destructuring-bind (&key target using)
                                    (cast-rule-matches rule source)
                                  (list target using))
         :do (when (and (null target) using (null first-match-using))
               (setf first-match-using using))
         :until target
         :finally (let ((coldef (make-pgsql-type source target using)))
                    (log-message :info "CAST ~a.~a ~a [~s, ~:[NULL~;NOT NULL~]~:[~*~;, ~a~]] TO ~s~@[ USING ~a~]"
                                 table-name column-name ctype default
                                 (string= "NO" nullable)
                                 (string/= "" extra) extra
                                 (format-column coldef)
                                 using)
                    (return coldef))))))


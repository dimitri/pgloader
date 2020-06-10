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

(defun parse-column-unsigned (data-type column-type)
  "See if we find the term unsigned in the column-type."
  (declare (ignore data-type))
  (when (search "unsigned" column-type :test #'string-equal) t))

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
                ((:signed rule-signed) nil s-s-p)
                ((:unsigned rule-unsigned) nil u-s-p)
		((:not-null rule-source-not-null) nil n-s-p)
		((:auto-increment rule-source-auto-increment))
		((:on-update-current-timestamp rule-source-updts))
		&allow-other-keys)
	rule-source
      (destructuring-bind (&key table-name
				column-name
				type
				ctype
				typemod
				default
				not-null
                                extra
                                unsigned
				auto-increment
                                on-update-current-timestamp)
	  source
	(declare (ignore ctype extra))
	(when
            (or
             ;; if we match by column, then table and column is all we
             ;; need to compare
             (and c-s-p
                  (string-equal table-name (car rule-source-column))
                  (string-equal column-name (cdr rule-source-column)))

             ;; otherwide, we do the full dance
             (and
              (or (and t-s-p (string-equal type rule-source-type)))
              (or (null tm-s-p) (when typemod
                                  (typemod-expr-matches-p typemod-expr typemod)))
              (or (null d-s-p)  (string-equal default rule-source-default))
              (or (null s-s-p)  (eq unsigned (not rule-signed)))
              (or (null u-s-p)  (eq unsigned rule-unsigned))
              (or (null n-s-p)  (eq not-null rule-source-not-null))

              ;; current RULE only matches SOURCE when both have an
              ;; auto_increment property, or none have it.
              (or (and auto-increment rule-source-auto-increment)
                  (and (not auto-increment) (not rule-source-auto-increment)))

              ;; current RULE only matches SOURCE when both have an
              ;; on-update-current-timestamp property, or none have it.
              (or (and on-update-current-timestamp rule-source-updts)
                  (and (not on-update-current-timestamp)
                       (not rule-source-updts)))))
	  (list :using using :target rule-target))))))

(defun make-pgsql-type (source target using)
  "Returns a COLUMN struct suitable for a PostgreSQL type definition"
  (destructuring-bind (&key ((:table-name source-table-name))
			    ((:column-name source-column-name))
			    ((:type source-type))
			    ((:ctype source-ctype))
			    ((:typemod source-typemod))
			    ((:default source-default))
                            ((:extra source-extra))
			    ((:not-null source-not-null))
			    &allow-other-keys)
      source
    (if target
	(destructuring-bind (&key type
                                  drop-extra
                                  drop-default
				  drop-not-null
                                  set-not-null
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
                         :nullable (and (not set-not-null)
                                        (or (not source-not-null)
                                            drop-not-null))
                         :default (when (and source-default (not drop-default))
                                    source-default)
                         :extra (when (and source-extra (not drop-extra))
                                  source-extra)
                         :transform using)))

	;; NO MATCH
	;;
	;; prefer char(24) over just char, that is the column type over the
	;; data type.
        (make-column :name (apply-identifier-case source-column-name)
                     :type-name source-ctype
                     :nullable (not source-not-null)
                     :default source-default
                     :extra source-extra
                     :transform using))))

(defun apply-casting-rules (table-name column-name
                            dtype ctype default nullable extra
                            &key
                              (rules (append *cast-rules*
                                             *default-cast-rules*)))
  "Apply the given RULES to the MySQL SOURCE type definition"
  (let* ((typemod        (parse-column-typemod dtype ctype))
         (unsigned       (parse-column-unsigned dtype ctype))
	 (not-null       (string-equal nullable "NO"))
	 (auto-increment (eq :auto-increment extra))
         (on-upd-cts     (eq :on-update-current-timestamp extra))
	 (source        `(:table-name ,table-name
                                      :column-name ,column-name
                                      :type ,dtype
                                      :ctype ,ctype
                                      ,@(when typemod (list :typemod typemod))
                                      :unsigned ,unsigned
                                      :default ,default
                                      :not-null ,not-null
                                      :extra ,extra
                                      :auto-increment ,auto-increment
                                      :on-update-current-timestamp ,on-upd-cts)))
    (let (first-match-using)
      (loop
         :for rule :in rules
         :for (target using) := (destructuring-bind (&key target using)
                                    (cast-rule-matches rule source)
                                  (list target using))
         :do (when (and (null target) using (null first-match-using))
               (setf first-match-using using))
         :until target
         :finally (return (make-pgsql-type source target using))))))


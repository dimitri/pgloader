;;;
;;; Now parsing CAST rules for migrating from MySQL
;;;

(in-package :pgloader.parser)

(defrule cast-typemod-guard (and kw-when sexp)
  (:destructure (w expr) (declare (ignore w)) (cons :typemod expr)))

(defrule cast-default-guard (and kw-when kw-default quoted-string)
  (:destructure (w d value) (declare (ignore w d)) (cons :default value)))

(defrule cast-unsigned-guard (and kw-when kw-unsigned)
  (:constant (cons :unsigned t)))

(defrule cast-signed-guard (and kw-when kw-signed)
  (:constant (cons :signed t)))

;; at the moment we only know about extra auto_increment
(defrule cast-source-extra (and kw-with kw-extra
                                (or kw-auto-increment
                                    kw-on-update-current-timestamp))
  (:lambda (extra)
    (cons (third extra) t)))

;; type names may be "double quoted"
(defrule cast-type-name (or double-quoted-namestring
                            (and (alpha-char-p character)
                                 (* (or (alpha-char-p character)
                                        (digit-char-p character)
                                        #\_))))
  (:text t))

(defrule cast-source-type (and kw-type cast-type-name)
  (:destructure (kw name) (declare (ignore kw)) (list :type name)))

(defrule table-column-name (and maybe-quoted-namestring
                                "."
                                maybe-quoted-namestring)
  (:destructure (table-name dot column-name)
    (declare (ignore dot))
    (list :column (cons (text table-name) (text column-name)))))

(defrule cast-source-column (and kw-column table-column-name)
  ;; well, we want namestring . namestring
  (:destructure (kw name) (declare (ignore kw)) name))

(defrule cast-source-extra-or-guard (* (or cast-unsigned-guard
                                           cast-signed-guard
                                           cast-default-guard
                                           cast-typemod-guard
                                           cast-source-extra))
  (:function alexandria:alist-plist))

(defrule cast-source (and (or cast-source-type cast-source-column)
                          cast-source-extra-or-guard)
  (:lambda (source)
    (bind (((name-and-type extra-and-guards) source)
           ((&key (default nil d-s-p)
                  (typemod nil t-s-p)
                  (signed nil s-s-p)
                  (unsigned nil u-s-p)
                  (auto-increment nil ai-s-p)
                  (on-update-current-timestamp nil ouct-s-p)
                  &allow-other-keys)
            extra-and-guards))
      `(,@name-and-type
		,@(when t-s-p (list :typemod typemod))
		,@(when d-s-p (list :default default))
		,@(when s-s-p (list :signed signed))
		,@(when u-s-p (list :unsigned unsigned))
		,@(when ai-s-p (list :auto-increment auto-increment))
                ,@(when ouct-s-p (list :on-update-current-timestamp
                                       on-update-current-timestamp))))))

(defrule cast-to-type (and kw-to cast-type-name ignore-whitespace)
  (:lambda (source)
    (bind (((_ type-name _) source))
      (list :type type-name))))

(defrule cast-keep-default  (and kw-keep kw-default)
  (:constant (list :drop-default nil)))

(defrule cast-keep-typemod (and kw-keep kw-typemod)
  (:constant (list :drop-typemod nil)))

(defrule cast-keep-not-null (and kw-keep kw-not kw-null)
  (:constant (list :drop-not-null nil)))

(defrule cast-drop-default  (and kw-drop kw-default)
  (:constant (list :drop-default t)))

(defrule cast-drop-typemod (and kw-drop kw-typemod)
  (:constant (list :drop-typemod t)))

(defrule cast-drop-not-null (and kw-drop kw-not kw-null)
  (:constant (list :drop-not-null t)))

(defrule cast-set-not-null (and kw-set kw-not kw-null)
  (:constant (list :set-not-null t)))

(defrule cast-keep-extra (and kw-keep kw-extra)
  (:constant (list :keep-extra t)))

(defrule cast-drop-extra (and kw-drop kw-extra)
  (:constant (list :drop-extra t)))

(defrule cast-def (+ (or cast-to-type
			 cast-keep-default
			 cast-drop-default
                         cast-keep-extra
                         cast-drop-extra
			 cast-keep-typemod
			 cast-drop-typemod
			 cast-keep-not-null
			 cast-drop-not-null
                         cast-set-not-null))
  (:lambda (source)
    (destructuring-bind
	  (&key type drop-default drop-extra drop-typemod
                drop-not-null set-not-null &allow-other-keys)
	(apply #'append source)
      (list :type type
	    :drop-extra drop-extra
	    :drop-default drop-default
	    :drop-typemod drop-typemod
	    :drop-not-null drop-not-null
            :set-not-null set-not-null))))

(defun function-name-character-p (char)
  (or (member char #.(quote (coerce "/.-%" 'list)))
      (alphanumericp char)))

(defrule function-name (+ (function-name-character-p character))
  (:lambda (fname)
    (text fname)))

(defrule package-and-function-names (and function-name
                                         (or ":" "::")
                                         function-name)
  (:lambda (pfn)
    (bind (((pname _ fname) pfn))
      (intern (string-upcase fname) (find-package (string-upcase pname))))))

(defrule maybe-qualified-function-name (or package-and-function-names
                                           function-name)
  (:lambda (fname)
    (typecase fname
      (string (intern (string-upcase fname) :pgloader.transforms))
      (symbol fname))))

(defrule transform-expression sexp
  (:lambda (sexp)
    (eval sexp)))

(defrule cast-function (and kw-using (or maybe-qualified-function-name
                                         transform-expression))
  (:destructure (using symbol) (declare (ignore using)) symbol))

(defun fix-target-type (source target)
  "When target has :type nil, steal the source :type definition."
  (if (getf target :type)
      target
      (loop
	 for (key value) on target by #'cddr
	 append (list key (if (eq :type key) (getf source :type) value)))))

(defrule cast-rule (and cast-source (? cast-def) (? cast-function))
  (:lambda (cast)
    (destructuring-bind (source target function) cast
      (list :source source
	    :target (fix-target-type source target)
	    :using function))))

(defrule another-cast-rule (and comma cast-rule)
  (:lambda (source)
    (bind (((_ rule) source)) rule)))

(defrule cast-rule-list (and cast-rule (* another-cast-rule))
  (:lambda (source)
    (destructuring-bind (rule1 rules) source
      (list* rule1 rules))))

(defrule casts (and kw-cast cast-rule-list)
  (:lambda (source)
    (bind (((_ casts) source))
      (cons :casts casts))))

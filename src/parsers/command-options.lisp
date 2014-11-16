;;;
;;; Parsing GUCs and WITH options for loading from MySQL and from file.
;;;
(in-package :pgloader.parser)

(defun optname-char-p (char)
  (and (or (alphanumericp char)
	   (char= char #\-)		; support GUCs
	   (char= char #\_))		; support GUCs
       (not (char= char #\Space))))

(defrule optname-element (* (optname-char-p character)))
(defrule another-optname-element (and keep-a-single-whitespace optname-element))

(defrule optname (and optname-element (* another-optname-element))
  (:lambda (source)
    (string-trim " " (text source))))

(defun optvalue-char-p (char)
  (not (member char '(#\, #\; #\=) :test #'char=)))

(defrule optvalue (+ (optvalue-char-p character))
  (:text t))

(defrule equal-sign (and (* whitespace) #\= (* whitespace))
  (:constant :equal))

(defrule option-workers (and kw-workers equal-sign (+ (digit-char-p character)))
  (:lambda (workers)
    (bind (((_ _ nb) workers))
      (cons :workers (parse-integer (text nb))))))

(defrule option-batch-rows (and kw-batch kw-rows equal-sign
                                (+ (digit-char-p character)))
  (:lambda (batch-rows)
    (bind (((_ _ _ nb) batch-rows))
      (cons :batch-rows (parse-integer (text nb))))))

(defrule byte-size-multiplier (or #\k #\M #\G #\T #\P)
  (:lambda (multiplier)
    (case (aref multiplier 0)
      (#\k 10)
      (#\M 20)
      (#\G 30)
      (#\T 40)
      (#\P 50))))

(defrule byte-size-unit (and ignore-whitespace (? byte-size-multiplier) #\B)
  (:lambda (unit)
    (bind (((_ &optional (multiplier 1) _) unit))
      (expt 2 multiplier))))

(defrule batch-size (and (+ (digit-char-p character)) byte-size-unit)
  (:lambda (batch-size)
    (destructuring-bind (nb unit) batch-size
      (* (parse-integer (text nb)) unit))))

(defrule option-batch-size (and kw-batch kw-size equal-sign batch-size)
  (:lambda (batch-size)
    (bind (((_ _ _ val) batch-size))
      (cons :batch-size val))))

(defrule option-batch-concurrency (and kw-batch kw-concurrency equal-sign
                                       (+ (digit-char-p character)))
  (:lambda (batch-concurrency)
    (bind (((_ _ _ nb) batch-concurrency))
      (cons :batch-concurrency (parse-integer (text nb))))))

(defun batch-control-bindings (options)
  "Generate the code needed to add batch-control"
  `((*copy-batch-rows*    (or ,(getf options :batch-rows) *copy-batch-rows*))
    (*copy-batch-size*    (or ,(getf options :batch-size) *copy-batch-size*))
    (*concurrent-batches* (or ,(getf options :batch-concurrency) *concurrent-batches*))))

(defun remove-batch-control-option (options
                                    &key
                                      (option-list '(:batch-rows
                                                     :batch-size
                                                     :batch-concurrency))
                                      extras)
  "Given a list of options, remove the generic ones that should already have
   been processed."
  (loop :for (k v) :on options :by #'cddr
     :unless (member k (append option-list extras))
     :append (list k v)))

(defmacro make-option-rule (name rule &optional option)
  "Generates a rule named NAME to parse RULE and return OPTION."
  (let* ((bindings
	  (loop for element in rule
	     unless (member element '(and or))
	     collect (if (and (typep element 'list)
			      (eq '? (car element))) 'no (gensym))))
	 (ignore (loop for b in bindings unless (eq 'no b) collect b))
	 (option-name (intern (string-upcase (format nil "option-~a" name))))
	 (option      (or option (intern (symbol-name name) :keyword))))
    `(defrule ,option-name ,rule
       (:destructure ,bindings
		     (declare (ignore ,@ignore))
		     (cons ,option (null no))))))

(make-option-rule include-drop    (and kw-include (? kw-no) kw-drop))
(make-option-rule truncate        (and (? kw-no) kw-truncate))
(make-option-rule create-tables   (and kw-create (? kw-no) kw-tables))
(make-option-rule create-indexes  (and kw-create (? kw-no) kw-indexes))
(make-option-rule reset-sequences (and kw-reset  (? kw-no) kw-sequences))
(make-option-rule foreign-keys    (and (? kw-no) kw-foreign kw-keys))

(defrule option-schema-only (and kw-schema kw-only)
  (:constant (cons :schema-only t)))

(defrule option-data-only (and kw-data kw-only)
  (:constant (cons :data-only t)))

(defrule option-identifiers-case (and (or kw-downcase kw-quote) kw-identifiers)
  (:lambda (id-case)
    (bind (((action _) id-case))
      (cons :identifier-case action))))

(defrule mysql-option (or option-workers
                          option-batch-rows
                          option-batch-size
                          option-batch-concurrency
			  option-truncate
			  option-data-only
			  option-schema-only
			  option-include-drop
			  option-create-tables
			  option-create-indexes
			  option-reset-sequences
			  option-foreign-keys
			  option-identifiers-case))

(defrule comma (and ignore-whitespace #\, ignore-whitespace)
  (:constant :comma))

(defrule another-mysql-option (and comma mysql-option)
  (:lambda (source)
    (bind (((_ option) source)) option)))

(defrule mysql-option-list (and mysql-option (* another-mysql-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist (list* opt1 opts)))))

(defrule mysql-options (and kw-with mysql-option-list)
  (:lambda (source)
    (bind (((_ opts) source))
      (cons :mysql-options opts))))

;; we don't validate GUCs, that's PostgreSQL job.
(defrule generic-optname optname-element
  (:text t))

(defrule generic-value (and #\' (* (not #\')) #\')
  (:lambda (quoted)
    (destructuring-bind (open value close) quoted
      (declare (ignore open close))
      (text value))))

(defrule generic-option (and generic-optname
			     (or equal-sign kw-to)
			     generic-value)
  (:lambda (source)
    (bind (((name _ value) source))
      (cons name value))))

(defrule another-generic-option (and comma generic-option)
  (:lambda (source)
    (bind (((_ option) source)) option)))

(defrule generic-option-list (and generic-option (* another-generic-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      ;; here we want an alist
      (list* opt1 opts))))

(defrule gucs (and kw-set generic-option-list)
  (:lambda (source)
    (bind (((_ gucs) source))
      (cons :gucs gucs))))

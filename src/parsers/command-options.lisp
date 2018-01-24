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
      (cons :worker-count (parse-integer (text nb))))))

(defrule option-concurrency (and kw-concurrency
                                 equal-sign
                                 (+ (digit-char-p character)))
  (:lambda (concurrency)
    (bind (((_ _ nb) concurrency))
      (cons :concurrency (parse-integer (text nb))))))

(defrule option-max-parallel-create-index
    (and kw-max kw-parallel kw-create kw-index equal-sign
         (+ (digit-char-p character)))
  (:lambda (opt)
    (cons :max-parallel-create-index (parse-integer (text (sixth opt))))))

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

;;; deprecated, but still accept it in the parsing
(defrule option-prefetch-rows (and (or (and kw-batch kw-concurrency)
                                       (and kw-prefetch kw-rows))
                                   equal-sign
                                   (+ (digit-char-p character)))
  (:lambda (prefetch-rows)
    (bind (((_ _ nb) prefetch-rows))
      (cons :prefetch-rows (parse-integer (text nb))))))

(defrule option-rows-per-range (and kw-rows kw-per kw-range
                                    equal-sign
                                    (+ (digit-char-p character)))
  (:lambda (rows-per-range)
    (cons :rows-per-range (parse-integer (text (fifth rows-per-range))))))

(defun batch-control-bindings (options)
  "Generate the code needed to add batch-control"
  `((*copy-batch-rows* (or ,(getf options :batch-rows) *copy-batch-rows*))
    (*copy-batch-size* (or ,(getf options :batch-size) *copy-batch-size*))
    (*prefetch-rows*   (or ,(getf options :prefetch-rows) *prefetch-rows*))
    (*rows-per-range*  (or ,(getf options :rows-per-range) *rows-per-range*))))

(defun identifier-case-binding (options)
  "Generate the code needed to bind *identifer-case* to the proper value."
  `((*identifier-case*  (or ,(getf options :identifier-case) *identifier-case*))))

(defun remove-batch-control-option (options
                                    &key
                                      (option-list '(:batch-rows
                                                     :batch-size
                                                     :prefetch-rows
                                                     :rows-per-range
                                                     :on-error-stop
                                                     :identifier-case))
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

(make-option-rule include-drop     (and kw-include (? kw-no) kw-drop))
(make-option-rule truncate         (and (? kw-no) kw-truncate))
(make-option-rule disable-triggers (and kw-disable (? kw-no) kw-triggers))
(make-option-rule drop-indexes     (and kw-drop (? kw-no) kw-indexes))
(make-option-rule create-tables    (and kw-create (? kw-no) kw-tables))
(make-option-rule create-indexes   (and kw-create (? kw-no) kw-indexes))
(make-option-rule reset-sequences  (and kw-reset  (? kw-no) kw-sequences))
(make-option-rule foreign-keys     (and (? kw-no) kw-foreign kw-keys))

(defrule option-drop-schema (and kw-drop kw-schema)
  (:constant (cons :drop-schema t)))

(defrule option-reindex (and kw-drop kw-indexes)
  (:constant (cons :reindex t)))

(defrule option-single-reader (and kw-single kw-reader kw-per kw-thread)
  (:constant (cons :multiple-readers nil)))

(defrule option-multiple-readers (and kw-multiple
                                      (or kw-readers kw-reader)
                                      kw-per kw-thread)
  (:constant (cons :multiple-readers t)))

(defrule option-schema-only (and kw-schema kw-only)
  (:constant (cons :schema-only t)))

(defrule option-data-only (and kw-data kw-only)
  (:constant (cons :data-only t)))

(defrule option-on-error-stop (and kw-on kw-error kw-stop)
  (:constant (cons :on-error-stop t)))

(defrule option-on-error-resume-next (and kw-on kw-error kw-resume kw-next)
  (:constant (cons :on-error-stop nil)))

(defrule option-identifiers-case (and (or kw-snake_case
                                          kw-downcase
                                          kw-quote)
                                      kw-identifiers)
  (:lambda (id-case)
    (bind (((action _) id-case))
      (cons :identifier-case action))))

(defrule option-index-names (and (or kw-preserve kw-uniquify) kw-index kw-names)
  (:lambda (preserve-or-uniquify)
    (bind (((action _ _) preserve-or-uniquify))
      (cons :index-names action))))

(defrule option-encoding (and kw-encoding encoding)
  (:lambda (enc)
    (cons :encoding
          (if enc
              (destructuring-bind (kw-encoding encoding) enc
                (declare (ignore kw-encoding))
                encoding)
              :utf-8))))

(defrule comma (and ignore-whitespace #\, ignore-whitespace)
  (:constant :comma))

(defun flatten-option-list (with-option-list)
  "Flatten given WITH-OPTION-LIST into a flat plist:

   Input: (:with
           ((:INCLUDE-DROP . T)
            ((:COMMA (:CREATE-TABLES . T)) (:COMMA (:CREATE-INDEXES . T))
             (:COMMA (:RESET-SEQUENCES . T)))))

   Output: (:INCLUDE-DROP T :CREATE-TABLES T
            :CREATE-INDEXES T :RESET-SEQUENCES T)"
  (destructuring-bind (with option-list) with-option-list
    (declare (ignore with))
    (cons :options
          (alexandria:alist-plist
           (append (list (first option-list))
                   (loop :for node :in (second option-list)
                      ;; bypass :comma
                      :append (cdr node)))))))


;;;
;;; PostgreSQL GUCs, another kind of options
;;;

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

(defrule gucs (and kw-set
                   (? (and kw-postgresql kw-parameters))
                   generic-option-list)
  (:lambda (source)
    (bind (((_ _ gucs) source))
      (cons :gucs gucs))))

;;;
;;; LOAD FIXED COLUMNS FILE
;;;
;;; That has lots in common with CSV, so we share a fair amount of parsing
;;; rules with the CSV case.
;;;

(in-package #:pgloader.parser)

(defrule option-fixed-header (and kw-fixed kw-header)
  (:constant (cons :header t)))

(defrule hex-number (and "0x" (+ (hexdigit-char-p character)))
  (:lambda (hex)
    (bind (((_ digits) hex))
      (parse-integer (text digits) :radix 16))))

(defrule dec-number (+ (digit-char-p character))
  (:lambda (digits)
    (parse-integer (text digits))))

(defrule number (or hex-number dec-number))

(defrule field-start-position (and (? kw-from) ignore-whitespace number)
  (:function third))

(defrule fixed-field-length (and (? kw-for) ignore-whitespace number)
  (:function third))

(defrule fixed-source-field (and csv-field-name
				 field-start-position fixed-field-length
				 csv-field-options)
  (:destructure (name start len opts)
    `(,name :start ,start :length ,len ,@opts)))

(defrule another-fixed-source-field (and comma fixed-source-field)
  (:lambda (source)
    (bind (((_ field) source)) field)))

(defrule fixed-source-fields (and fixed-source-field (* another-fixed-source-field))
  (:lambda (source)
    (destructuring-bind (field1 fields) source
      (list* field1 fields))))

(defrule fixed-source-field-list (and open-paren fixed-source-fields close-paren)
  (:lambda (source)
    (bind (((_ field-defs _) source)) field-defs)))

(defrule fixed-option (or option-on-error-stop
                          option-on-error-resume-next
                          option-workers
                          option-concurrency
                          option-batch-rows
                          option-batch-size
                          option-prefetch-rows
                          option-max-parallel-create-index
                          option-truncate
                          option-drop-indexes
                          option-disable-triggers
                          option-identifiers-case
			  option-skip-header
                          option-fixed-header))

(defrule fixed-options (and kw-with
                            (and fixed-option (* (and comma fixed-option))))
  (:function flatten-option-list))

(defrule fixed-uri (and "fixed://" filename)
  (:lambda (source)
    (bind (((_ filename) source))
      (make-instance 'fixed-connection :spec filename))))

(defrule fixed-file-source (or stdin
			       inline
                               http-uri
                               fixed-uri
			       filename-matching
			       maybe-quoted-filename)
  (:lambda (src)
    (if (typep src 'fixed-connection) src
        (destructuring-bind (type &rest specs) src
          (case type
            (:stdin    (make-instance 'fixed-connection :spec src))
            (:inline   (make-instance 'fixed-connection :spec src))
            (:filename (make-instance 'fixed-connection :spec src))
            (:regex    (make-instance 'fixed-connection :spec src))
            (:http     (make-instance 'fixed-connection :uri (first specs))))))))

(defrule fixed-source (and kw-load kw-fixed kw-from fixed-file-source)
  (:lambda (src)
    (bind (((_ _ _ source) src)) source)))

(defrule load-fixed-cols-file-optional-clauses (* (or fixed-options
                                                      gucs
                                                      before-load
                                                      after-load))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule load-fixed-cols-file-command (and fixed-source (? file-encoding)
                                           (? fixed-source-field-list)
                                           target
                                           (? csv-target-table)
                                           (? csv-target-column-list)
                                           load-fixed-cols-file-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source encoding fields pguri table-name columns clauses)
        command
      (list* source
             encoding
             fields
             pguri
             (or table-name (pgconn-table-name pguri))
             columns
             clauses))))

(defun lisp-code-for-loading-from-fixed (fixed-conn pg-db-conn
                                         &key
                                           (encoding :utf-8)
                                           fields
                                           target-table-name
                                           columns
                                           gucs before after options
                                         &allow-other-keys
                                         &aux
                                           (worker-count (getf options :worker-count))
                                           (concurrency  (getf options :concurrency)))
  `(lambda ()
     (let* (,@(pgsql-connection-bindings pg-db-conn gucs)
            ,@(batch-control-bindings options)
              ,@(identifier-case-binding options)
              (source-db (with-stats-collection ("fetch" :section :pre)
                           (expand (fetch-file ,fixed-conn)))))

       (progn
         ,(sql-code-block pg-db-conn :pre before "before load")

         (let ((on-error-stop                 ,(getf options :on-error-stop))
               (truncate                      ,(getf options :truncate))
               (disable-triggers              ,(getf options :disable-triggers))
               (drop-indexes                  ,(getf options :drop-indexes))
               (max-parallel-create-index     ,(getf options :max-parallel-create-index))
               (source
                (make-instance 'copy-fixed
                               :target-db ,pg-db-conn
                               :source source-db
                               :target (create-table ',target-table-name)
                               :encoding ,encoding
                               :fields ',fields
                               :columns ',columns
                               :skip-lines ,(or (getf options :skip-lines) 0)
                               :header ,(getf options :header))))

           (copy-database source
                          ,@ (when worker-count
                               (list :worker-count worker-count))
                          ,@ (when concurrency
                               (list :concurrency concurrency))
                          :on-error-stop on-error-stop
                          :truncate truncate
                          :drop-indexes drop-indexes
                          :disable-triggers disable-triggers
                          :max-parallel-create-index max-parallel-create-index))

         ,(sql-code-block pg-db-conn :post after "after load")))))

(defrule load-fixed-cols-file load-fixed-cols-file-command
  (:lambda (command)
    (bind (((source encoding fields pg-db-uri table-name columns
                    &key options gucs before after) command))
      (cond (*dry-run*
             (lisp-code-for-csv-dry-run pg-db-uri))
            (t
             (lisp-code-for-loading-from-fixed source pg-db-uri
                                               :encoding encoding
                                               :fields fields
                                               :target-table-name table-name
                                               :columns columns
                                               :gucs gucs
                                               :before before
                                               :after after
                                               :options options))))))

;;;
;;; LOAD COPY FILE
;;;
;;; That has lots in common with CSV, so we share a fair amount of parsing
;;; rules with the CSV case.
;;;

(in-package #:pgloader.parser)

(defrule copy-source-field csv-field-name
  (:lambda (field-name)
    (list field-name)))

(defrule another-copy-source-field (and comma copy-source-field)
  (:lambda (source)
    (bind (((_ field) source)) field)))

(defrule copy-source-fields (and copy-source-field (* another-copy-source-field))
  (:lambda (source)
    (destructuring-bind (field1 fields) source
      (list* field1 fields))))

(defrule copy-source-field-list (and open-paren copy-source-fields close-paren)
  (:lambda (source)
    (bind (((_ field-defs _) source)) field-defs)))

(defrule option-delimiter (and kw-delimiter separator)
  (:lambda (delimiter)
    (destructuring-bind (kw sep) delimiter
      (declare (ignore kw))
      (cons :delimiter sep))))

(defrule option-null (and kw-null quoted-string)
  (:destructure (kw null) (declare (ignore kw)) (cons :null-as null)))

(defrule copy-option (or option-batch-rows
                         option-batch-size
                         option-batch-concurrency
                         option-truncate
                         option-drop-indexes
                         option-disable-triggers
                         option-skip-header
                         option-delimiter
                         option-null))

(defrule another-copy-option (and comma copy-option)
  (:lambda (source)
    (bind (((_ option) source)) option)))

(defrule copy-option-list (and copy-option (* another-copy-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist `(,opt1 ,@opts)))))

(defrule copy-options (and kw-with copy-option-list)
  (:lambda (source)
    (bind (((_ opts) source))
      (cons :copy-options opts))))

(defrule copy-uri (and "copy://" filename)
  (:lambda (source)
    (bind (((_ filename) source))
      (make-instance 'copy-connection :spec filename))))

(defrule copy-file-source (or stdin
                              inline
                              http-uri
                              copy-uri
                              filename-matching
                              maybe-quoted-filename)
  (:lambda (src)
    (if (typep src 'copy-connection) src
        (destructuring-bind (type &rest specs) src
          (case type
            (:stdin    (make-instance 'copy-connection :spec src))
            (:inline   (make-instance 'copy-connection :spec src))
            (:filename (make-instance 'copy-connection :spec src))
            (:regex    (make-instance 'copy-connection :spec src))
            (:http     (make-instance 'copy-connection :uri (first specs))))))))

(defrule get-copy-file-source-from-environment-variable (and kw-getenv name)
  (:lambda (p-e-v)
    (bind (((_ varname) p-e-v)
           (connstring (getenv-default varname)))
      (unless connstring
          (error "Environment variable ~s is unset." varname))
        (parse 'copy-file-source connstring))))

(defrule copy-source (and kw-load kw-copy kw-from
                          (or get-copy-file-source-from-environment-variable
                              copy-file-source))
  (:lambda (src)
    (bind (((_ _ _ source) src)) source)))

(defrule load-copy-file-optional-clauses (* (or copy-options
                                                gucs
                                                before-load
                                                after-load))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule load-copy-file-command (and copy-source (? file-encoding)
                                     (? copy-source-field-list)
                                     target
                                     (? csv-target-column-list)
                                     load-copy-file-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source encoding fields target columns clauses) command
      `(,source ,encoding ,fields ,target ,columns ,@clauses))))

(defun lisp-code-for-loading-from-copy (copy-conn fields pg-db-conn
                                        &key
                                          (encoding :utf-8)
                                          columns
                                          gucs before after
                                          ((:copy-options options)))
  `(lambda ()
     (let* (,@(pgsql-connection-bindings pg-db-conn gucs)
            ,@(batch-control-bindings options)
            (source-db     (with-stats-collection ("fetch" :section :pre)
                               (expand (fetch-file ,copy-conn)))))

       (progn
         ,(sql-code-block pg-db-conn :pre before "before load")

         (let ((truncate ,(getf options :truncate))
               (disable-triggers (getf ',options :disable-triggers))
               (drop-indexes     (getf ',options :drop-indexes))
               (source
                (make-instance 'pgloader.copy:copy-copy
                               :target-db ,pg-db-conn
                               :source source-db
                               :target ',(pgconn-table-name pg-db-conn)
                               :encoding ,encoding
                               :fields ',fields
                               :columns ',columns
                               ,@(remove-batch-control-option
                                  options :extras '(:truncate
                                                    :drop-indexes
                                                    :disable-triggers)))))
           (pgloader.sources:copy-from source
                                       :truncate truncate
                                       :drop-indexes drop-indexes
                                       :disable-triggers disable-triggers))

         ,(sql-code-block pg-db-conn :post after "after load")))))

(defrule load-copy-file load-copy-file-command
  (:lambda (command)
    (bind (((source encoding fields pg-db-uri columns
                    &key ((:copy-options options)) gucs before after) command))
      (cond (*dry-run*
             (lisp-code-for-csv-dry-run pg-db-uri))
            (t
             (lisp-code-for-loading-from-copy source fields pg-db-uri
                                              :encoding encoding
                                              :columns columns
                                              :gucs gucs
                                              :before before
                                              :after after
                                              :copy-options options))))))

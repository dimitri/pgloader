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

(defrule copy-option (or option-on-error-stop
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
                         option-delimiter
                         option-null))

(defrule copy-options (and kw-with
                           (and copy-option (* (and comma copy-option))))
  (:function flatten-option-list))

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

(defrule copy-source (and kw-load kw-copy kw-from copy-file-source)
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
                                     (? csv-target-table)
                                     (? csv-target-column-list)
                                     load-copy-file-optional-clauses)
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

(defun lisp-code-for-loading-from-copy (copy-conn pg-db-conn
                                        &key
                                          (encoding :utf-8)
                                          fields
                                          target-table-name
                                          columns
                                          gucs before after options
                                        &aux
                                          (worker-count (getf options :worker-count))
                                          (concurrency  (getf options :concurrency)))
  `(lambda ()
     (let* (,@(pgsql-connection-bindings pg-db-conn gucs)
            ,@(batch-control-bindings options)
              ,@(identifier-case-binding options)
              (source-db (with-stats-collection ("fetch" :section :pre)
                           (expand (fetch-file ,copy-conn)))))

       (progn
         ,(sql-code-block pg-db-conn :pre before "before load")

         (let ((on-error-stop             (getf ',options :on-error-stop))
               (truncate                  (getf ',options :truncate))
               (disable-triggers          (getf ',options :disable-triggers))
               (drop-indexes              (getf ',options :drop-indexes))
               (max-parallel-create-index (getf ',options :max-parallel-create-index))
               (source
                (make-instance 'copy-copy
                               :target-db ,pg-db-conn
                               :source    source-db
                               :target    (create-table ',target-table-name)
                               :encoding  ,encoding
                               :fields   ',fields
                               :columns  ',columns
                               ,@(remove-batch-control-option
                                  options :extras '(:worker-count
                                                    :concurrency
                                                    :truncate
                                                    :drop-indexes
                                                    :disable-triggers
                                                    :max-parallel-create-index)))))
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

(defrule load-copy-file load-copy-file-command
  (:lambda (command)
    (bind (((source encoding fields pg-db-uri table-name columns
                    &key options gucs before after) command))
      (cond (*dry-run*
             (lisp-code-for-csv-dry-run pg-db-uri))
            (t
             (lisp-code-for-loading-from-copy source pg-db-uri
                                              :encoding encoding
                                              :fields fields
                                              :target-table-name table-name
                                              :columns columns
                                              :gucs gucs
                                              :before before
                                              :after after
                                              :options options))))))

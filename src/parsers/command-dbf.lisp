#|
    LOAD DBF FROM '/Users/dim/Downloads/comsimp2013.dbf'
        INTO postgresql://dim@localhost:54393/dim?comsimp2013
        WITH truncate, create table, table name = 'comsimp2013'
|#

(in-package #:pgloader.parser)

(defrule option-create-table (and kw-create kw-table)
  (:constant (cons :create-tables t)))

(defrule quoted-table-name (and #\' (or qualified-table-name namestring) #\')
  (:lambda (qtn)
    (bind (((_ name _) qtn)) name)))

(defrule option-table-name (and kw-table kw-name equal-sign quoted-table-name)
  (:lambda (tn)
    (bind (((_ _ _ table-name) tn))
      (cons :table-name (text table-name)))))

(defrule dbf-option (or option-on-error-stop
                        option-on-error-resume-next
                        option-workers
                        option-concurrency
                        option-batch-rows
                        option-batch-size
                        option-prefetch-rows
                        option-truncate
                        option-disable-triggers
                        option-data-only
                        option-schema-only
                        option-include-drop
                        option-create-table
                        option-create-tables
                        option-table-name
                        option-identifiers-case))

(defrule dbf-options (and kw-with (and dbf-option (* (and comma dbf-option))))
  (:function flatten-option-list))

(defrule dbf-uri (and "dbf://" filename)
  (:lambda (source)
    (bind (((_ filename) source))
      (make-instance 'dbf-connection :path (second filename)))))

(defrule dbf-file-source (or dbf-uri filename-or-http-uri)
  (:lambda (conn-or-path-or-uri)
    (if (typep conn-or-path-or-uri 'dbf-connection) conn-or-path-or-uri
        (destructuring-bind (kind url) conn-or-path-or-uri
          (case kind
            (:filename (make-instance 'dbf-connection :path url))
            (:http     (make-instance 'dbf-connection :uri url)))))))

(defrule dbf-source (and kw-load kw-dbf kw-from dbf-file-source)
  (:lambda (src)
    (bind (((_ _ _ source) src)) source)))

(defrule load-dbf-optional-clauses (* (or dbf-options
                                          gucs
                                          casts
                                          before-load
                                          after-schema
                                          after-load))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

;;; dbf defaults to ascii rather than utf-8
(defrule dbf-file-encoding (? (and kw-with kw-encoding encoding))
  (:lambda (enc)
    (when enc
      (bind (((_ _ encoding) enc)) encoding))))

(defrule load-dbf-command (and dbf-source
                               (? dbf-file-encoding)
                               target
                               (? csv-target-table)
                               load-dbf-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source encoding pguri table-name clauses)
        command
      (list* source
             encoding
             pguri
             (or table-name (pgconn-table-name pguri))
             clauses))))

(defun lisp-code-for-dbf-dry-run (dbf-db-conn pg-db-conn)
  `(lambda ()
     (let ((source-db (expand (fetch-file ,dbf-db-conn))))
       (check-connection source-db)
       (check-connection ,pg-db-conn))))

(defun lisp-code-for-loading-from-dbf (dbf-db-conn pg-db-conn
                                       &key
                                         target-table-name
                                         encoding
                                         gucs casts options
                                         before after-schema after
                                         &allow-other-keys)
  `(lambda ()
     (let* ((*default-cast-rules* ',*db3-default-cast-rules*)
            (*cast-rules*         ',casts)
            ,@(pgsql-connection-bindings pg-db-conn gucs)
            ,@(batch-control-bindings options)
              ,@(identifier-case-binding options)
              (on-error-stop (getf ',options :on-error-stop))
              (source-db     (with-stats-collection ("fetch" :section :pre)
                               (expand (fetch-file ,dbf-db-conn))))
              (source
               (make-instance 'copy-db3
                              :target-db ,pg-db-conn
                              :encoding ,encoding
                              :source-db source-db
                              :target ,(when target-table-name
                                         (create-table target-table-name)))))

       ,(sql-code-block pg-db-conn :pre before "before load")

       (copy-database source
                      ,@(remove-batch-control-option options)
                      :after-schema ',after-schema
                      :on-error-stop on-error-stop
                      :create-indexes nil
                      :foreign-keys nil
                      :reset-sequences nil)

       ,(sql-code-block pg-db-conn :post after "after load"))))

(defrule load-dbf-file load-dbf-command
  (:lambda (command)
    (bind (((source encoding pg-db-uri table-name
                    &key options gucs casts before after-schema after)
            command))
      (cond (*dry-run*
             (lisp-code-for-dbf-dry-run source pg-db-uri))
            (t
             (lisp-code-for-loading-from-dbf source pg-db-uri
                                             :target-table-name table-name
                                             :encoding encoding
                                             :gucs gucs
                                             :casts casts
                                             :before before
                                             :after-schema after-schema
                                             :after after
                                             :options options))))))

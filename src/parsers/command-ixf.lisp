#|
    LOAD IXF FROM '/Users/dim/Downloads/comsimp2013.ixf'
        INTO postgresql://dim@localhost:54393/dim?comsimp2013
        WITH truncate, create table, table name = 'comsimp2013'
|#

(in-package #:pgloader.parser)

(defrule tz-utc (~ "UTC") (:constant local-time:+utc-zone+))
(defrule tz-gmt (~ "GMT") (:constant local-time:+gmt-zone+))
(defrule tz-name (and #\' (+ (not #\')) #\')
  (:lambda (tzn)
    (bind (((_ chars _) tzn))
      (local-time:reread-timezone-repository)
      (local-time:find-timezone-by-location-name (text chars)))))

(defrule option-timezone (and kw-timezone (or tz-utc tz-gmt tz-name))
  (:lambda (tzopt)
    (bind (((_ tz) tzopt)) (cons :timezone tz))))

(defrule ixf-option (or option-on-error-stop
                        option-on-error-resume-next
                        option-workers
                        option-concurrency
                        option-batch-rows
                        option-batch-size
                        option-prefetch-rows
                        option-truncate
                        option-disable-triggers
                        option-identifiers-case
                        option-data-only
                        option-schema-only
                        option-include-drop
                        option-create-table
                        option-create-tables
                        option-table-name
                        option-timezone))

(defrule ixf-options (and kw-with (and ixf-option (* (and comma ixf-option))))
  (:function flatten-option-list))

(defrule ixf-uri (and "ixf://" filename)
  (:lambda (source)
    (bind (((_ filename) source))
      (make-instance 'ixf-connection :path (second filename)))))

(defrule ixf-file-source (or ixf-uri filename-or-http-uri)
  (:lambda (conn-or-path-or-uri)
    (if (typep conn-or-path-or-uri 'ixf-connection) conn-or-path-or-uri
        (destructuring-bind (kind url) conn-or-path-or-uri
          (case kind
            (:filename (make-instance 'ixf-connection :path url))
            (:http     (make-instance 'ixf-connection :uri url)))))))

(defrule ixf-source (and kw-load kw-ixf kw-from ixf-file-source)
  (:lambda (src)
    (bind (((_ _ _ source) src)) source)))

(defrule load-ixf-optional-clauses (* (or ixf-options
                                          gucs
                                          before-load
                                          after-schema
                                          after-load))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule load-ixf-command (and ixf-source
                               target
                               (? csv-target-table)
                               load-ixf-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source pguri table-name clauses) command
      (list* source
             pguri
             (or table-name (pgconn-table-name pguri))
             clauses))))

(defun lisp-code-for-loading-from-ixf (ixf-db-conn pg-db-conn
                                       &key
                                         target-table-name gucs options
                                         before after-schema after
                                         &allow-other-keys)
  `(lambda ()
     (let* (,@(pgsql-connection-bindings pg-db-conn gucs)
            ,@(batch-control-bindings options)
              ,@(identifier-case-binding options)
              (timezone     (getf ',options :timezone))
              (on-error-stop(getf ',options :on-error-stop))
              (source-db    (with-stats-collection ("fetch" :section :pre)
                              (expand (fetch-file ,ixf-db-conn))))
              (source
               (make-instance 'copy-ixf
                              :target-db ,pg-db-conn
                              :source-db source-db
                              :target (create-table ',target-table-name)
                              :timezone timezone)))

       ,(sql-code-block pg-db-conn :pre before "before load")

       (copy-database source
                      ,@(remove-batch-control-option
                         options
                         :extras '(:timezone))
                      :on-error-stop on-error-stop
                      :after-schema ',after-schema
                      :foreign-keys nil
                      :reset-sequences nil)

       ,(sql-code-block pg-db-conn :post after "after load"))))

(defrule load-ixf-file load-ixf-command
  (:lambda (command)
    (bind (((source pg-db-uri table-name
                    &key options gucs before after-schema after)
            command))
      (cond (*dry-run*
             (lisp-code-for-csv-dry-run pg-db-uri))
            (t
             (lisp-code-for-loading-from-ixf source pg-db-uri
                                             :target-table-name table-name
                                             :gucs gucs
                                             :before before
                                             :after-schema after-schema
                                             :after after
                                             :options options))))))

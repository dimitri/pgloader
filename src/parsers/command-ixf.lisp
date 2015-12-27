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

(defrule ixf-option (or option-batch-rows
                        option-batch-size
                        option-batch-concurrency
                        option-truncate
                        option-disable-triggers
                        option-data-only
                        option-schema-only
                        option-include-drop
                        option-create-table
                        option-create-tables
                        option-table-name
                        option-timezone))

(defrule another-ixf-option (and comma ixf-option)
  (:lambda (source)
    (bind (((_ option) source)) option)))

(defrule ixf-option-list (and ixf-option (* another-ixf-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist `(,opt1 ,@opts)))))

;;; piggyback on DBF parsing
(defrule ixf-options (and kw-with ixf-option-list)
  (:lambda (source)
    (bind (((_ opts) source))
      (cons :ixf-options opts))))

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
                                          after-load))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule load-ixf-command (and ixf-source target load-ixf-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source target clauses) command
      `(,source ,target ,@clauses))))

(defun lisp-code-for-loading-from-ixf (ixf-db-conn pg-db-conn
                                       &key
                                         gucs before after
                                         ((:ixf-options options)))
  `(lambda ()
     (let* (,@(pgsql-connection-bindings pg-db-conn gucs)
            ,@(batch-control-bindings options)
            ,@(identifier-case-binding options)
            (timezone     (getf ',options :timezone))
            (table-name   (create-table ',(pgconn-table-name pg-db-conn)))
            (source-db    (with-stats-collection ("fetch" :section :pre)
                              (expand (fetch-file ,ixf-db-conn))))
            (source
             (make-instance 'pgloader.ixf:copy-ixf
                            :target-db ,pg-db-conn
                            :source-db source-db
                            :target table-name
                            :timezone timezone)))

       ,(sql-code-block pg-db-conn :pre before "before load")

       (pgloader.sources:copy-database source
                                       ,@(remove-batch-control-option
                                          options
                                          :extras '(:timezone)))

       ,(sql-code-block pg-db-conn :post after "after load"))))

(defrule load-ixf-file load-ixf-command
  (:lambda (command)
    (bind (((source pg-db-uri
                    &key ((:ixf-options options)) gucs before after) command))
      (cond (*dry-run*
             (lisp-code-for-csv-dry-run pg-db-uri))
            (t
             (lisp-code-for-loading-from-ixf source pg-db-uri
                                             :gucs gucs
                                             :before before
                                             :after after
                                             :ixf-options options))))))

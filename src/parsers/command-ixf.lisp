#|
    LOAD IXF FROM '/Users/dim/Downloads/comsimp2013.ixf'
        INTO postgresql://dim@localhost:54393/dim?comsimp2013
        WITH truncate, create table, table name = 'comsimp2013'
|#

(in-package #:pgloader.parser)

(defrule option-create-table (and kw-create kw-table)
  (:constant (cons :create-tables t)))

;;; piggyback on DBF parsing
(defrule ixf-options (and kw-with dbf-option-list)
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
            (table-name   ',(pgconn-table-name pg-db-conn))
            (source-db      (with-stats-collection ("fetch" :section :pre)
                              (expand (fetch-file ,ixf-db-conn))))
            (source
             (make-instance 'pgloader.ixf:copy-ixf
                            :target-db ,pg-db-conn
                            :source-db source-db
                            :target table-name)))

       ,(sql-code-block pg-db-conn :pre before "before load")

       (pgloader.sources:copy-database source
                                       ,@(remove-batch-control-option options))

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

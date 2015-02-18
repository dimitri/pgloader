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

(defrule dbf-option (or option-batch-rows
                        option-batch-size
                        option-batch-concurrency
                        option-truncate
                        option-data-only
                        option-schema-only
                        option-include-drop
                        option-create-table
                        option-create-tables
                        option-table-name))

(defrule another-dbf-option (and comma dbf-option)
  (:lambda (source)
    (bind (((_ option) source)) option)))

(defrule dbf-option-list (and dbf-option (* another-dbf-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist `(,opt1 ,@opts)))))

(defrule dbf-options (and kw-with dbf-option-list)
  (:lambda (source)
    (bind (((_ opts) source))
      (cons :dbf-options opts))))

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
                                          before-load
                                          after-load))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

;;; dbf defaults to ascii rather than utf-8
(defrule dbf-file-encoding (? (and kw-with kw-encoding encoding))
  (:lambda (enc)
    (if enc
        (bind (((_ _ encoding) enc)) encoding)
	:ascii)))

(defrule load-dbf-command (and dbf-source (? dbf-file-encoding)
                               target load-dbf-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source encoding target clauses) command
      `(,source ,encoding ,target ,@clauses))))

(defun lisp-code-for-loading-from-dbf (dbf-db-conn pg-db-conn
                                       &key
                                         (encoding :ascii)
                                         gucs before after
                                         ((:dbf-options options)))
  `(lambda ()
     (let* ((state-before   (pgloader.utils:make-pgstate))
            (summary        (null *state*))
            (*state*        (or *state* (pgloader.utils:make-pgstate)))
            (state-after   ,(when after `(pgloader.utils:make-pgstate)))
            ,@(pgsql-connection-bindings pg-db-conn gucs)
            ,@(batch-control-bindings options)
            ,@(identifier-case-binding options)
            (table-name    ,(or (getf options :table-name)
                                (pgconn-table-name pg-db-conn)))
            (source-db     (with-stats-collection ("fetch" :state state-before)
                             (expand (fetch-file ,dbf-db-conn))))
            (source
             (make-instance 'pgloader.db3:copy-db3
                            :target-db ,pg-db-conn
                            :encoding ,encoding
                            :source-db source-db
                            :target table-name)))

       ,(sql-code-block pg-db-conn 'state-before before "before load")

       (pgloader.sources:copy-database source
                                       :state-before state-before
                                       ,@(remove-batch-control-option options))

       ,(sql-code-block pg-db-conn 'state-after after "after load")

       ;; reporting
       (when summary
         (report-full-summary "Total import time" *state*
                              :before state-before
                              :finally state-after)))))

(defrule load-dbf-file load-dbf-command
  (:lambda (command)
    (bind (((source encoding pg-db-uri
                    &key ((:dbf-options options)) gucs before after) command))
      (lisp-code-for-loading-from-dbf source pg-db-uri
                                      :encoding encoding
                                      :gucs gucs
                                      :before before
                                      :after after
                                      :dbf-options options))))

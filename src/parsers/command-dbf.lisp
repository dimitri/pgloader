#|
    LOAD DBF FROM '/Users/dim/Downloads/comsimp2013.dbf'
        INTO postgresql://dim@localhost:54393/dim?comsimp2013
        WITH truncate, create table, table name = 'comsimp2013'
|#

(in-package #:pgloader.parser)

(defrule option-create-table (and kw-create kw-table)
  (:constant (cons :create-table t)))

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
                        option-create-table
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

(defrule dbf-source (and kw-load kw-dbf kw-from filename-or-http-uri)
  (:lambda (src)
    (bind (((_ _ _ source) src)) source)))

(defrule load-dbf-optional-clauses (* (or dbf-options
                                          gucs
                                          before-load
                                          after-load))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule load-dbf-command (and dbf-source target load-dbf-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source target clauses) command
      `(,source ,target ,@clauses))))

(defrule load-dbf-file load-dbf-command
  (:lambda (command)
    (bind (((source pg-db-uri
                    &key ((:dbf-options options)) gucs before after) command)
           ((&key dbname table-name &allow-other-keys)               pg-db-uri))
      `(lambda ()
         (let* ((state-before   (pgloader.utils:make-pgstate))
                (summary        (null *state*))
                (*state*        (or *state* (pgloader.utils:make-pgstate)))
                (state-after   ,(when after `(pgloader.utils:make-pgstate)))
                ,@(pgsql-connection-bindings pg-db-uri gucs)
                ,@(batch-control-bindings options)
                (source
                 ,(bind (((kind url) source))
                        (ecase kind
                          (:http     `(with-stats-collection
                                          ("download" :state state-before)
                                        (pgloader.archive:http-fetch-file ,url)))
                          (:filename url))))
                (source
                 (if (string= "zip" (pathname-type source))
                     (progn
                       (with-stats-collection ("extract" :state state-before)
                         (let ((d (pgloader.archive:expand-archive source)))
                           (make-pathname :defaults d
                                          :name (pathname-name source)
                                          :type "dbf"))))
                     source))
                (source
                 (make-instance 'pgloader.db3:copy-db3
                                :target-db ,dbname
                                :source source
                                :target ,table-name)))

           ,(sql-code-block dbname 'state-before before "before load")

           (pgloader.sources:copy-from source
                                       :state-before state-before
                                       ,@(remove-batch-control-option options))

           ,(sql-code-block dbname 'state-after after "after load")

           ;; reporting
           (when summary
             (report-full-summary "Total import time" *state*
                                  :before state-before
                                  :finally state-after)))))))

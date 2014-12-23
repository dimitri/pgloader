#|
    LOAD IXF FROM '/Users/dim/Downloads/comsimp2013.ixf'
        INTO postgresql://dim@localhost:54393/dim?comsimp2013
        WITH truncate, create table, table name = 'comsimp2013'
|#

(in-package #:pgloader.parser)

(defrule option-create-table (and kw-create kw-table)
  (:constant (cons :create-table t)))

;;; piggyback on DBF parsing
(defrule ixf-options (and kw-with dbf-option-list)
  (:lambda (source)
    (bind (((_ opts) source))
      (cons :ixf-options opts))))

(defrule ixf-source (and kw-load kw-ixf kw-from filename-or-http-uri)
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

(defun lisp-code-for-loading-from-ixf (source pg-db-uri
                                       &key
                                         gucs before after
                                         ((:ixf-options options)))
  (bind (((&key dbname table-name &allow-other-keys) pg-db-uri))
    `(lambda ()
       (let* ((state-before   (pgloader.utils:make-pgstate))
              (summary        (null *state*))
              (*state*        (or *state* (pgloader.utils:make-pgstate)))
              (state-after   ,(when after `(pgloader.utils:make-pgstate)))
              ,@(pgsql-connection-bindings pg-db-uri gucs)
              ,@(batch-control-bindings options)
              ,@(identifier-case-binding options)
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
                                        :type "ixf"))))
                   source))
              (source
               (make-instance 'pgloader.ixf:copy-ixf
                              :target-db ,dbname
                              :source source
                              :target ,table-name)))

         ,(sql-code-block dbname 'state-before before "before load")

         (pgloader.sources:copy-from source
                                     :state-before state-before
                                     ,@(remove-batch-control-option options))

         ,(sql-code-block dbname 'state-after after "after load")

         (when summary
           (report-full-summary "Total import time" *state*
                                :before state-before
                                :finally state-after))))))

(defrule load-ixf-file load-ixf-command
  (:lambda (command)
    (bind (((source pg-db-uri
                    &key ((:ixf-options options)) gucs before after) command))
      (lisp-code-for-loading-from-ixf source pg-db-uri
                                      :gucs gucs
                                      :before before
                                      :after after
                                      :ixf-options options))))

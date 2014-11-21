;;;
;;; Parse the pgloader commands grammar
;;;

(in-package :pgloader.parser)

(defun mssql-connection-bindings (ms-db-uri)
  "Generate the code needed to set MSSQL connection bindings."
  (destructuring-bind (&key ((:host mshost))
                            ((:port msport))
                            ((:user msuser))
                            ((:password mspass))
                            ((:dbname msdb))
                            &allow-other-keys)
      ms-db-uri
    `((*msconn-host* ',mshost)
      (*msconn-port* ,msport)
      (*msconn-user* ,msuser)
      (*msconn-pass* ,mspass)
      (*ms-dbname*   ,msdb))))


;;;
;;; Allow clauses to appear in any order
;;;
(defrule load-mssql-optional-clauses (* (or mysql-options
                                            gucs
                                            casts
                                            before-load
                                            after-load))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule mssql-prefix "mssql://" (:constant (list :type :mssql)))

(defrule mssql-uri (and mssql-prefix
                        (? dsn-user-password)
                        (? dsn-hostname)
                        dsn-dbname)
  (:lambda (uri)
    (destructuring-bind (&key type
                              user
			      password
			      host
			      port
			      dbname)
        (apply #'append uri)
      ;; Default to environment variables as described in
      ;;  http://www.freetds.org/userguide/envvar.htm
      (list :type      type
            :user      (or user     (getenv-default "USER"))
            :password  password
            :host      (or host     (getenv-default "TDSHOST" "localhost"))
            :port      (or port     (parse-integer
                                     (getenv-default "TDSPORT" "1433")))
            :dbname    dbname))))

(defrule get-mssql-uri-from-environment-variable (and kw-getenv name)
  (:lambda (p-e-v)
    (bind (((_ varname) p-e-v))
      (let ((connstring (getenv-default varname)))
        (unless connstring
          (error "Environment variable ~s is unset." varname))
        (parse 'mssql-uri connstring)))))

(defrule mssql-source (and kw-load kw-database kw-from
                           (or mssql-uri
                               get-mssql-uri-from-environment-variable))
  (:lambda (source) (bind (((_ _ _ uri) source)) uri)))

(defrule load-mssql-command (and mssql-source target
                                 load-mssql-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source target clauses) command
      `(,source ,target ,@clauses))))


;;; LOAD DATABASE FROM mssql://
(defrule load-mssql-database load-mssql-command
  (:lambda (source)
    (bind (((ms-db-uri pg-db-uri
                       &key
                       gucs casts before after
                       ((:mssql-options options)))           source)

           ((&key ((:dbname msdb)) table-name
                  &allow-other-keys)                        ms-db-uri)

           ((&key ((:dbname pgdb)) &allow-other-keys)       pg-db-uri))
      `(lambda ()
         (let* ((state-before  (pgloader.utils:make-pgstate))
                (*state*       (or *state* (pgloader.utils:make-pgstate)))
                (state-idx     (pgloader.utils:make-pgstate))
                (state-after   (pgloader.utils:make-pgstate))
                (*default-cast-rules* ',*mssql-default-cast-rules*)
                (*cast-rules*         ',casts)
                ,@(mssql-connection-bindings ms-db-uri)
                ,@(pgsql-connection-bindings pg-db-uri gucs)
                ,@(batch-control-bindings options)
                ,@(identifier-case-binding options)
                (source
                 (make-instance 'pgloader.mssql::copy-mssql
                                :target-db ,pgdb
                                :source-db ,msdb)))

           ,(sql-code-block pgdb 'state-before before "before load")

           (pgloader.mssql:copy-database source
                                         ,@(when table-name
                                                 `(:only-tables ',(list table-name)))
                                         :state-before state-before
                                         :state-after state-after
                                         :state-indexes state-idx
                                         ,@(remove-batch-control-option options))

           ,(sql-code-block pgdb 'state-after after "after load")

           (report-full-summary "Total import time" *state*
                                :before   state-before
                                :finally  state-after
                                :parallel state-idx))))))


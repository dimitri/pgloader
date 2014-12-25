;;;
;;; Parse the pgloader commands grammar
;;;

(in-package :pgloader.parser)

;;;
;;; INCLUDING ONLY and EXCLUDING clauses for MS SQL
;;;
;;; There's no regexp matching on MS SQL, so we're going to just use the
;;; classic LIKE support here, as documented at:
;;;
;;;  http://msdn.microsoft.com/en-us/library/ms187489(SQL.90).aspx
;;;
(defrule like-expression (and "'" (+ (not "'")) "'")
  (:lambda (le)
    (bind (((_ like _) le)) (text like))))

(defrule another-like-expression (and comma like-expression)
  (:lambda (source)
    (bind (((_ like) source)) like)))

(defrule filter-list-like (and like-expression (* another-like-expression))
  (:lambda (source)
    (destructuring-bind (filter1 filters) source
      (list* filter1 filters))))

(defrule including-in-schema
    (and kw-including kw-only kw-table kw-names kw-like filter-list-like
         kw-in kw-schema quoted-namestring)
  (:lambda (source)
    (bind (((_ _ _ _ _ filter-list _ _ schema) source))
      (cons schema filter-list))))

(defrule including-like (and including-in-schema (* including-in-schema))
  (:lambda (source)
    (destructuring-bind (inc1 incs) source
      (cons :including (list* inc1 incs)))))

(defrule excluding-in-schema
    (and kw-excluding kw-table kw-names kw-like filter-list-like
         kw-in kw-schema quoted-namestring)
  (:lambda (source)
    (bind (((_ _ _ _ filter-list _ _ schema) source))
      (cons schema filter-list))))

(defrule excluding-like (and excluding-in-schema (* excluding-in-schema))
  (:lambda (source)
    (destructuring-bind (excl1 excls) source
      (cons :excluding (list* excl1 excls)))))


;;;
;;; Allow clauses to appear in any order
;;;
(defrule load-mssql-optional-clauses (* (or mysql-options
                                            gucs
                                            casts
                                            before-load
                                            after-load
                                            including-like
                                            excluding-like))
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
      (declare (ignore type))
      (make-instance 'mssql-connection
                     :user (or user (getenv-default "USER"))
                     :pass password
                     :host (or host (getenv-default "TDSHOST" "localhost"))
                     :port (or port (parse-integer
                                     (getenv-default "TDSPORT" "1433")))
                     :name    dbname))))

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
(defun lisp-code-for-loading-from-mssql (ms-db-conn pg-db-conn
                                         &key
                                           gucs casts before after
                                           ((:mssql-options options))
                                           (including)
                                           (excluding))
  `(lambda ()
     (let* ((state-before  (pgloader.utils:make-pgstate))
            (*state*       (or *state* (pgloader.utils:make-pgstate)))
            (state-idx     (pgloader.utils:make-pgstate))
            (state-after   (pgloader.utils:make-pgstate))
            (*default-cast-rules* ',*mssql-default-cast-rules*)
            (*cast-rules*         ',casts)
            ,@(pgsql-connection-bindings pg-db-conn gucs)
            ,@(batch-control-bindings options)
            ,@(identifier-case-binding options)
            (source
             (make-instance 'pgloader.mssql::copy-mssql
                            :target-db ,pg-db-conn
                            :source-db ,ms-db-conn)))

       ,(sql-code-block pg-db-conn 'state-before before "before load")

       (pgloader.mssql:copy-database source
                                     :state-before state-before
                                     :state-after state-after
                                     :state-indexes state-idx
                                     :including ',including
                                     :excluding ',excluding
                                     ,@(remove-batch-control-option options))

       ,(sql-code-block pg-db-conn 'state-after after "after load")

       (report-full-summary "Total import time" *state*
                            :before   state-before
                            :finally  state-after
                            :parallel state-idx))))

(defrule load-mssql-database load-mssql-command
  (:lambda (source)
    (bind (((ms-db-uri pg-db-uri
                       &key
                       gucs casts before after including excluding
                       ((:mysql-options options)))
            source))
      (lisp-code-for-loading-from-mssql ms-db-uri pg-db-uri
                                        :gucs gucs
                                        :casts casts
                                        :before before
                                        :after after
                                        :mssql-options options
                                        :including including
                                        :excluding excluding))))


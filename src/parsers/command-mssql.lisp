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
(make-option-rule create-schemas (and kw-create (? kw-no) kw-schemas))

(defrule mssql-option (or option-on-error-stop
                          option-on-error-resume-next
                          option-workers
                          option-concurrency
                          option-batch-rows
                          option-batch-size
                          option-prefetch-rows
                          option-max-parallel-create-index
                          option-reindex
                          option-truncate
                          option-disable-triggers
                          option-data-only
                          option-schema-only
                          option-include-drop
                          option-create-tables
                          option-create-schemas
                          option-create-indexes
			  option-index-names
                          option-reset-sequences
			  option-foreign-keys
                          option-encoding
                          option-identifiers-case))

(defrule mssql-options (and kw-with
                            (and mssql-option (* (and comma mssql-option))))
  (:function flatten-option-list))

(defrule including-in-schema
    (and kw-including kw-only kw-table kw-names kw-like filter-list-like
         kw-in kw-schema quoted-namestring)
  (:lambda (source)
    (bind (((_ _ _ _ _ filter-list _ _ schema) source))
      (cons schema filter-list))))

(defrule including-like-in-schema
    (and including-in-schema (* including-in-schema))
  (:lambda (source)
    (destructuring-bind (inc1 incs) source
      (cons :including (list* inc1 incs)))))

(defrule excluding-in-schema
    (and kw-excluding kw-table kw-names kw-like filter-list-like
         kw-in kw-schema quoted-namestring)
  (:lambda (source)
    (bind (((_ _ _ _ filter-list _ _ schema) source))
      (cons schema filter-list))))

(defrule excluding-like-in-schema
    (and excluding-in-schema (* excluding-in-schema))
  (:lambda (source)
    (destructuring-bind (excl1 excls) source
      (cons :excluding (list* excl1 excls)))))


;;;
;;; MSSQL SET parameters, because sometimes we need that
;;;
(defrule mssql-gucs (and kw-set kw-mssql kw-parameters generic-option-list)
  (:lambda (mygucs) (cons :mssql-gucs (fourth mygucs))))


;;;
;;; Allow clauses to appear in any order
;;;
(defrule load-mssql-optional-clauses (* (or mssql-options
                                            mssql-gucs
                                            gucs
                                            casts
                                            alter-schema
                                            alter-table
                                            materialize-views
                                            distribute-commands
                                            before-load
                                            after-schema
                                            after-load
                                            including-like-in-schema
                                            excluding-like-in-schema))
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

(defrule mssql-source (and kw-load kw-database kw-from mssql-uri)
  (:lambda (source) (bind (((_ _ _ uri) source)) uri)))

(defrule load-mssql-command (and mssql-source target
                                 load-mssql-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source target clauses) command
      `(,source ,target ,@clauses))))


;;; LOAD DATABASE FROM mssql://
(defun lisp-code-for-mssql-dry-run (ms-db-conn pg-db-conn)
  `(lambda ()
     ;; now is the time to load the CFFI lib we need (freetds)
     (log-message :log "Loading the FreeTDS shared librairy (sybdb)")
     (cffi:load-foreign-library 'mssql::sybdb)

     (log-message :log "DRY RUN, only checking connections.")
     (check-connection ,ms-db-conn)
     (check-connection ,pg-db-conn)))

(defun lisp-code-for-loading-from-mssql (ms-db-conn pg-db-conn
                                         &key
                                           gucs mssql-gucs
                                           casts before after after-schema
                                           options distribute views
                                           alter-schema alter-table
                                           including excluding
                                           &allow-other-keys)
  `(lambda ()
     ;; now is the time to load the CFFI lib we need (freetds)
     (let (#+sbcl(sb-ext:*muffled-warnings* 'style-warning))
       (cffi:load-foreign-library 'mssql::sybdb))

     (let* ((*default-cast-rules* ',*mssql-default-cast-rules*)
            (*cast-rules*         ',casts)
            (*mssql-settings*     ',mssql-gucs)
            (on-error-stop        (getf ',options :on-error-stop t))
            ,@(pgsql-connection-bindings pg-db-conn gucs)
            ,@(batch-control-bindings options)
            ,@(identifier-case-binding options)
            (source
             (make-instance 'copy-mssql
                            :target-db ,pg-db-conn
                            :source-db ,ms-db-conn)))

       ,(sql-code-block pg-db-conn :pre before "before load")

       (copy-database source
                      :including ',including
                      :excluding ',excluding
                      :alter-schema ',alter-schema
                      :alter-table ',alter-table
                      :after-schema ',after-schema
                      :materialize-views ',views
                      :distribute ',distribute
                      :set-table-oids t
                      :on-error-stop on-error-stop
                      ,@(remove-batch-control-option options))

       ,(sql-code-block pg-db-conn :post after "after load"))))

(defrule load-mssql-database load-mssql-command
  (:lambda (source)
    (bind (((ms-db-uri pg-db-uri
                       &key
                       gucs mssql-gucs casts views before after-schema after
                       alter-schema alter-table distribute
                       including excluding options)
            source))
      (cond (*dry-run*
             (lisp-code-for-mssql-dry-run ms-db-uri pg-db-uri))
            (t
             (lisp-code-for-loading-from-mssql ms-db-uri pg-db-uri
                                               :gucs gucs
                                               :mssql-gucs mssql-gucs
                                               :casts casts
                                               :views views
                                               :before before
                                               :after-schema after-schema
                                               :after after
                                               :alter-schema alter-schema
                                               :alter-table alter-table
                                               :distribute distribute
                                               :options options
                                               :including including
                                               :excluding excluding))))))


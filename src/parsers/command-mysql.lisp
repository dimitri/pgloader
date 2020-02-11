;;;
;;; Parse the pgloader commands grammar
;;;

(in-package :pgloader.parser)

;;;
;;; MySQL options
;;;
(defrule mysql-option (or option-on-error-stop
                          option-on-error-resume-next
                          option-workers
                          option-concurrency
                          option-batch-rows
                          option-batch-size
                          option-prefetch-rows
                          option-max-parallel-create-index
                          option-single-reader
                          option-multiple-readers
                          option-rows-per-range
			  option-reindex
                          option-truncate
                          option-disable-triggers
			  option-data-only
			  option-schema-only
			  option-include-drop
                          option-drop-schema
			  option-create-tables
			  option-create-indexes
			  option-index-names
			  option-reset-sequences
			  option-foreign-keys
			  option-identifiers-case))

(defrule mysql-options (and kw-with
                            (and mysql-option (* (and comma mysql-option))))
  (:function flatten-option-list))


;;;
;;; Including only some tables or excluding some others
;;;
(defrule including-matching
    (and kw-including kw-only kw-table kw-names kw-matching filter-list-matching)
  (:lambda (source)
    (bind (((_ _ _ _ _ filter-list) source))
      (cons :including filter-list))))

(defrule excluding-matching
    (and kw-excluding kw-table kw-names kw-matching filter-list-matching)
  (:lambda (source)
    (bind (((_ _ _ _ filter-list) source))
      (cons :excluding filter-list))))


;;;
;;; Per table encoding options, because MySQL is so bad at encoding...
;;;
(defrule decoding-table-as (and kw-decoding kw-table kw-names kw-matching
                                filter-list-matching
                                kw-as encoding)
  (:lambda (source)
    (bind (((_ _ _ _ filter-list _ encoding) source))
      (cons encoding filter-list))))

(defrule decoding-tables-as (+ decoding-table-as)
  (:lambda (tables)
    (cons :decoding tables)))


;;;
;;; MySQL SET parameters, because sometimes we need that
;;;
(defrule mysql-gucs (and kw-set kw-mysql kw-parameters generic-option-list)
  (:lambda (mygucs) (cons :mysql-gucs (fourth mygucs))))


;;;
;;; Allow clauses to appear in any order
;;;
(defrule load-mysql-optional-clauses (* (or mysql-options
                                            mysql-gucs
                                            gucs
                                            casts
                                            alter-table
                                            alter-schema
                                            materialize-views
                                            including-matching
                                            excluding-matching
                                            decoding-tables-as
                                            before-load
                                            after-schema
                                            after-load
                                            distribute-commands))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule mysql-prefix "mysql://" (:constant (list :type :mysql)))

(defrule mysql-dsn-option-usessl-true  "true"  (:constant :yes))
(defrule mysql-dsn-option-usessl-false "false" (:constant :no))

(defrule mysql-dsn-option-usessl (and "useSSL" "="
                                      (or mysql-dsn-option-usessl-true
                                          mysql-dsn-option-usessl-false))
  (:lambda (ssl)
    (cons :use-ssl (third ssl))))

(defrule mysql-dsn-option (or dsn-option-ssl
                              mysql-dsn-option-usessl
                              dsn-option-host
                              dsn-option-port
                              dsn-option-dbname
                              dsn-option-user
                              dsn-option-pass
                              dsn-option-table-name))

(defrule mysql-dsn-options (and "?" (and mysql-dsn-option
                                         (* (and "&" mysql-dsn-option))))
  (:lambda (opts) (cdr (flatten-option-list opts))))

(defrule mysql-uri (and mysql-prefix
                        (? dsn-user-password)
                        (? dsn-hostname)
                        dsn-dbname
                        (? mysql-dsn-options))
  (:lambda (uri)
    (destructuring-bind (&key type
                              user
			      password
			      host
			      port
			      dbname
                              (use-ssl :no))
        (apply #'append uri)
      ;; Default to environment variables as described in
      ;;  http://dev.mysql.com/doc/refman/5.0/en/environment-variables.html
      (declare (ignore type))
      (make-instance 'mysql-connection
                     :user (or user     (getenv-default "USER"))
                     :pass (or password (getenv-default "MYSQL_PWD"))
                     :host (or host     (getenv-default "MYSQL_HOST" "localhost"))
                     :port (or port     (parse-integer
                                         (getenv-default "MYSQL_TCP_PORT" "3306")))
                     :name dbname
                     :use-ssl use-ssl))))

(defrule mysql-source (and kw-load kw-database kw-from mysql-uri)
  (:lambda (source) (bind (((_ _ _ uri) source)) uri)))

(defrule load-mysql-command (and mysql-source target
                                 load-mysql-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source target clauses) command
      `(,source ,target ,@clauses))))


;;; LOAD DATABASE FROM mysql://
(defun lisp-code-for-mysql-dry-run (my-db-conn pg-db-conn)
  `(lambda ()
     (log-message :log "DRY RUN, only checking connections.")
     (check-connection ,my-db-conn)
     (check-connection ,pg-db-conn)))

(defun lisp-code-for-loading-from-mysql (my-db-conn pg-db-conn
                                         &key
                                           gucs mysql-gucs
                                           casts options views
                                           before after after-schema
                                           alter-table alter-schema
                                           distribute
                                           ((:including incl))
                                           ((:excluding excl))
                                           ((:decoding decoding-as))
                                           &allow-other-keys)
  `(lambda ()
     (let* ((*default-cast-rules* ',*mysql-default-cast-rules*)
            (*cast-rules*         ',casts)
            (*decoding-as*        ',decoding-as)
            (*mysql-settings*     ',mysql-gucs)
            (on-error-stop        (getf ',options :on-error-stop t))
            ,@(pgsql-connection-bindings pg-db-conn gucs)
            ,@(batch-control-bindings options)
            ,@(identifier-case-binding options)
            (source
             (make-instance 'copy-mysql
                            :target-db ,pg-db-conn
                            :source-db ,my-db-conn)))

       ,(sql-code-block pg-db-conn :pre before "before load")

       (copy-database source
                      :including ',incl
                      :excluding ',excl
                      :materialize-views ',views
                      :alter-table ',alter-table
                      :alter-schema ',alter-schema
                      :after-schema ',after-schema
                      :distribute ',distribute
                      :set-table-oids t
                      :on-error-stop on-error-stop
                      ,@(remove-batch-control-option options))

       ,(sql-code-block pg-db-conn :post after "after load"))))

(defrule load-mysql-database load-mysql-command
  (:lambda (source)
    (destructuring-bind (my-db-uri
                         pg-db-uri
                         &key
                         gucs mysql-gucs casts views before after after-schema
                         options alter-table alter-schema distribute
                         including excluding decoding)
        source
      (cond (*dry-run*
             (lisp-code-for-mysql-dry-run my-db-uri pg-db-uri))
            (t
             (lisp-code-for-loading-from-mysql my-db-uri pg-db-uri
                                               :gucs gucs
                                               :mysql-gucs mysql-gucs
                                               :casts casts
                                               :views views
                                               :before before
                                               :after after
                                               :after-schema after-schema
                                               :options options
                                               :alter-table alter-table
                                               :alter-schema alter-schema
                                               :distribute distribute
                                               :including including
                                               :excluding excluding
                                               :decoding decoding))))))


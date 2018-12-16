;;;
;;; Parse the pgloader commands grammar
;;;

(in-package :pgloader.parser)

;;;
;;; PostgreSQL options
;;;
(defrule pgsql-option (or option-on-error-stop
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
                          option-drop-schema
			  option-create-tables
			  option-create-indexes
			  option-index-names
			  option-reset-sequences
			  option-foreign-keys
			  option-identifiers-case))

(defrule pgsql-options (and kw-with
                            (and pgsql-option (* (and comma pgsql-option))))
  (:function flatten-option-list))


;;;
;;; Including only some tables or excluding some others
;;;
(defrule including-matching-in-schema-filter
    (and kw-including kw-only kw-table kw-names kw-matching filter-list-matching
         kw-in kw-schema quoted-namestring)
  (:lambda (source)
    (bind (((_ _ _ _ _ filter-list _ _ schema) source))
      (cons schema filter-list))))

(defrule including-matching-in-schema
    (and including-matching-in-schema-filter
         (* including-matching-in-schema-filter))
  (:lambda (source)
    (destructuring-bind (inc1 incs) source
      (cons :including (list* inc1 incs)))))

(defrule excluding-matching-in-schema-filter
    (and kw-excluding kw-table kw-names kw-matching filter-list-matching
         kw-in kw-schema quoted-namestring)
  (:lambda (source)
    (bind (((_ _ _ _ filter-list _ _ schema) source))
      (cons schema filter-list))))

(defrule excluding-matching-in-schema
    (and excluding-matching-in-schema-filter
         (* excluding-matching-in-schema-filter))
  (:lambda (source)
    (destructuring-bind (excl1 excls) source
      (cons :excluding (list* excl1 excls)))))


;;;
;;; Allow clauses to appear in any order
;;;
(defrule load-pgsql-optional-clauses (* (or pgsql-options
                                            gucs
                                            casts
                                            alter-table
                                            alter-schema
                                            materialize-views
                                            including-matching-in-schema
                                            excluding-matching-in-schema
                                            decoding-tables-as
                                            before-load
                                            after-schema
                                            after-load
                                            distribute-commands))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule pgsql-source (and kw-load kw-database kw-from pgsql-uri)
  (:lambda (source) (bind (((_ _ _ uri) source)) uri)))

(defrule load-pgsql-command (and pgsql-source target
                                 load-pgsql-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source target clauses) command
      `(,source ,target ,@clauses))))


;;; LOAD DATABASE FROM pgsql://
(defun lisp-code-for-pgsql-dry-run (pg-src-db-conn pg-dst-db-conn)
  `(lambda ()
     (log-message :log "DRY RUN, only checking connections.")
     (check-connection ,pg-src-db-conn)
     (check-connection ,pg-dst-db-conn)))

(defun lisp-code-for-loading-from-pgsql (pg-src-db-conn pg-dst-db-conn
                                         &key
                                           gucs
                                           casts options
                                           before after after-schema
                                           alter-table alter-schema
                                           ((:including incl))
                                           ((:excluding excl))
                                           views
                                           distribute
                                           &allow-other-keys)
  `(lambda ()
     (let* ((*default-cast-rules* ',*pgsql-default-cast-rules*)
            (*cast-rules*         ',casts)
            (*identifier-case*    :quote)
            (on-error-stop        (getf ',options :on-error-stop t))
            ,@(pgsql-connection-bindings pg-dst-db-conn gucs)
            ,@(batch-control-bindings options)
            (source
             (make-instance 'copy-pgsql
                            :target-db ,pg-dst-db-conn
                            :source-db ,pg-src-db-conn)))

       ,(sql-code-block pg-dst-db-conn :pre before "before load")

       (copy-database source
                      :including ',incl
                      :excluding ',excl
                      :materialize-views ',views
                      :alter-table ',alter-table
                      :alter-schema ',alter-schema
                      :index-names :preserve
                      :set-table-oids t
                      :on-error-stop on-error-stop
                      :after-schema ',after-schema
                      :distribute ',distribute
                      ,@(remove-batch-control-option options))

       ,(sql-code-block pg-dst-db-conn :post after "after load"))))

(defrule load-pgsql-database load-pgsql-command
  (:lambda (source)
    (destructuring-bind (pg-src-db-uri
                         pg-dst-db-uri
                         &key
                         gucs casts before after after-schema options
                         alter-table alter-schema views distribute
                         including excluding decoding)
        source
      (cond (*dry-run*
             (lisp-code-for-pgsql-dry-run pg-src-db-uri pg-dst-db-uri))
            (t
             (lisp-code-for-loading-from-pgsql pg-src-db-uri pg-dst-db-uri
                                               :gucs gucs
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


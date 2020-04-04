;;;
;;; Generic API for pgloader sources
;;; Methods for database source types (with introspection)
;;;

(in-package :pgloader.load)

;;;
;;; Prepare the PostgreSQL database before streaming the data into it.
;;;
(defmethod prepare-pgsql-database ((copy db-copy)
                                   (catalog catalog)
                                   &key
                                     truncate
                                     create-tables
                                     create-schemas
                                     drop-schema
                                     drop-indexes
                                     set-table-oids
                                     materialize-views
                                     foreign-keys
                                     include-drop)
  "Prepare the target PostgreSQL database: create tables casting datatypes
   from the MySQL definitions, prepare index definitions and create target
   tables for materialized views.

   That function mutates index definitions in ALL-INDEXES."
  (log-message :notice "Prepare PostgreSQL database.")

  (with-pgsql-transaction (:pgconn (target-db copy))

    (finalize-catalogs catalog (pgconn-variant (target-db copy)))

    (if create-tables
        (progn
          (when create-schemas
            (with-stats-collection ("Create Schemas" :section :pre
                                                     :use-result-as-read t
                                                     :use-result-as-rows t)
              (create-schemas catalog
                              :include-drop drop-schema
                              :client-min-messages :error)))

          ;; create new SQL types (ENUMs, SETs) if needed and before we
          ;; get to the table definitions that will use them
          (with-stats-collection ("Create SQL Types" :section :pre
                                                     :use-result-as-read t
                                                     :use-result-as-rows t)
            ;; some SQL types come from extensions (ip4r, hstore, etc)
            (create-extensions catalog
                               :include-drop include-drop
                               :if-not-exists t
                               :client-min-messages :error)

            (create-sqltypes catalog
                             :include-drop include-drop
                             :client-min-messages :error))

          ;; now the tables
          (with-stats-collection ("Create tables" :section :pre
                                                  :use-result-as-read t
                                                  :use-result-as-rows t)
            (create-tables catalog
                           :include-drop include-drop
                           :client-min-messages :error)))

        (progn
          ;; if we're not going to create the tables, now is the time to
          ;; remove the constraints: indexes, primary keys, foreign keys
          ;;
          ;; to be able to do that properly, get the constraints from
          ;; the pre-existing target database catalog
          (let* ((pgversion   (pgconn-major-version (target-db copy)))
                 (pgsql-catalog
                  (fetch-pgsql-catalog (db-name (target-db copy))
                                       :source-catalog catalog
                                       :pgversion pgversion)))
            (merge-catalogs catalog pgsql-catalog))

          ;; now the foreign keys and only then the indexes, because a
          ;; drop constraint on a primary key cascades to the drop of
          ;; any foreign key that targets the primary key
          (when foreign-keys
            (with-stats-collection ("Drop Foreign Keys" :section :pre
                                                        :use-result-as-read t
                                                        :use-result-as-rows t)
              (drop-pgsql-fkeys catalog :log-level :notice)))

          (when drop-indexes
            (with-stats-collection ("Drop Indexes" :section :pre
                                                   :use-result-as-read t
                                                   :use-result-as-rows t)
              ;; we want to error out early in case we can't DROP the
              ;; index, don't CASCADE
              (drop-indexes catalog :cascade nil :log-level :notice)))

          (when truncate
            (with-stats-collection ("Truncate" :section :pre
                                               :use-result-as-read t
                                               :use-result-as-rows t)
              (truncate-tables catalog)))))

    ;; Some database sources allow the same index name being used
    ;; against several tables, so we add the PostgreSQL table OID in the
    ;; index name, to differenciate. Set the table oids now.
    (when (and create-tables set-table-oids)
      (with-stats-collection ("Set Table OIDs" :section :pre
                                               :use-result-as-read t
                                               :use-result-as-rows t)
        (set-table-oids catalog :variant (pgconn-variant (target-db copy)))))

    ;; We might have to MATERIALIZE VIEWS
    (when (and create-tables materialize-views)
      (with-stats-collection ("Create MatViews Tables" :section :pre
                                                       :use-result-as-read t
                                                       :use-result-as-rows t)
        (create-views catalog
                      :include-drop include-drop
                      :client-min-messages :error))))

  ;; Citus Support
  ;;
  ;; We need a separate transaction here in some cases, because of the
  ;; distributed DDL support from Citus, to avoid the following error:
  ;;
  ;; ERROR Database error 25001: cannot establish a new connection for
  ;; placement 2299, since DDL has been executed on a connection that is in
  ;; use
  ;;
  (when (catalog-distribution-rules catalog)
    (with-pgsql-transaction (:pgconn (target-db copy))
      (with-stats-collection ("Citus Distribute Tables" :section :pre)
        (create-distributed-table (catalog-distribution-rules catalog)))))

  ;; log the catalog we just fetched and (maybe) merged
  (log-message :data "CATALOG: ~s" catalog))


(defmethod complete-pgsql-database ((copy db-copy)
                                    (catalog catalog)
                                    pkeys
                                    &key
                                      foreign-keys
                                      create-indexes
                                      create-triggers
                                      reset-sequences)
  "After loading the data into PostgreSQL, we can now reset the sequences
     and declare foreign keys."
  ;;
  ;; Now Reset Sequences, the good time to do that is once the whole data
  ;; has been imported and once we have the indexes in place, as max() is
  ;; able to benefit from the indexes. In particular avoid doing that step
  ;; while CREATE INDEX statements are in flight (avoid locking).
  ;;
  (log-message :notice "Completing PostgreSQL database.")

  (when reset-sequences
    (reset-sequences (clone-connection (target-db copy)) catalog))

  (handler-case
      (with-pgsql-transaction (:pgconn (clone-connection (target-db copy)))
        ;;
        ;; Turn UNIQUE indexes into PRIMARY KEYS now
        ;;
        (when create-indexes
          (pgsql-execute-with-timing :post "Primary Keys" pkeys
                                     :log-level :notice))

        ;;
        ;; Foreign Key Constraints
        ;;
        ;; We need to have finished loading both the reference and the
        ;; refering tables to be able to build the foreign keys, so wait
        ;; until all tables and indexes are imported before doing that.
        ;;
        (when foreign-keys
          (create-pgsql-fkeys catalog
                              :section :post
                              :label "Create Foreign Keys"
                              :log-level :notice))

        ;;
        ;; Triggers and stored procedures -- includes special default values
        ;;
        (when create-triggers
          (create-triggers catalog
                           :section :post
                           :label "Create Triggers"))

        ;;
        ;; Add schemas that needs to be in the search_path to the database
        ;; search_path, when using PostgreSQL. Redshift doesn't know how to
        ;; do that, unfortunately.
        ;;
        (unless (eq :redshift (pgconn-variant (target-db copy)))
          (add-to-search-path catalog
                              :section :post
                              :label "Set Search Path"))

        ;;
        ;; And now, comments on tables and columns.
        ;;
        (comment-on-tables-and-columns catalog
                                       :section :post
                                       :label "Install Comments"))

    (postgresql-unavailable (condition)

      (log-message :error "~a" condition)
      (log-message :error
                   "Complete PostgreSQL database reconnecting to PostgreSQL.")

      ;; in order to avoid Socket error in "connect": ECONNREFUSED if we
      ;; try just too soon, wait a little
      (sleep 2)


      ;;
      ;; Reset Sequence can be done several times safely, and the rest of the
      ;; operations run in a single transaction, so if the connection was lost,
      ;; nothing has been done. Retry.
      ;;
      (complete-pgsql-database copy
                               catalog
                               pkeys
                               :foreign-keys foreign-keys
                               :create-indexes create-indexes
                               :create-triggers create-triggers
                               :reset-sequences reset-sequences))))


(defun process-catalog (copy catalog &key alter-table alter-schema distribute)
  "Do all the PostgreSQL catalog tweaking here: casts, index WHERE clause
   rewriting, pgloader level alter schema and alter table commands."
  (log-message :info "Processing source catalogs")

  ;; cast the catalog into something PostgreSQL can work on
  (cast catalog)

  ;; support code for index filters (where clauses)
  (process-index-definitions catalog :sql-dialect (class-name (class-of copy)))

  ;; we may have to alter schemas
  (when alter-schema
    (alter-schema catalog alter-schema))

  ;; if asked, now alter the catalog with given rules: the alter-table
  ;; keyword parameter actually contains a set of alter table rules.
  (when alter-table
    (alter-table catalog alter-table))

  ;; we also support schema changes necessary for Citus distribution
  (when distribute
    (log-message :info "Applying distribution rules")
    (setf (catalog-distribution-rules catalog)
          (citus-distribute-schema catalog distribute))))

(defun optimize-table-copy-ordering (catalog)
  "Return a list of tables to copy over in optimized order"
  (let ((table-list (copy-list (table-list catalog)))
        (view-list  (copy-list (view-list catalog))))
    ;; when materialized views are not supported, view-list is empty here
    (cond
      ((notevery #'zerop (mapcar #'table-row-count-estimate table-list))
       (let ((sorted-table-list
              (sort table-list #'> :key #'table-row-count-estimate)))
         (log-message :notice
                      "Processing tables in this order: ~{~a: ~d rows~^, ~}"
                      (loop :for table :in (append table-list view-list)
                         :collect (format-table-name table)
                         :collect (table-row-count-estimate table)))
         (nconc sorted-table-list view-list)))
      (t
       (nconc table-list view-list)))))


;;;
;;; Generic enough implementation of the copy-database method.
;;;
(defmethod copy-database ((copy db-copy)
			  &key
                            (on-error-stop    *on-error-stop*)
                            (worker-count     4)
                            (concurrency      1)
                            (multiple-readers nil)
                            max-parallel-create-index
			    (truncate         nil)
			    (disable-triggers nil)
			    (data-only        nil)
			    (schema-only      nil)
                            (create-schemas   t)
			    (create-tables    t)
			    (include-drop     t)
                            (drop-schema      nil)
			    (create-indexes   t)
                            (index-names      :uniquify)
			    (reset-sequences  t)
			    (foreign-keys     t)
                            (reindex          nil)
                            (after-schema     nil)
                            distribute
			    including
			    excluding
                            set-table-oids
                            alter-table
                            alter-schema
			    materialize-views)
  "Export database source data and Import it into PostgreSQL"
  (log-message :log "Migrating from ~a" (source-db copy))
  (log-message :log "Migrating into ~a" (target-db copy))
  (let* ((*on-error-stop* on-error-stop)
         (copy-data      (or data-only (not schema-only)))
         (create-ddl     (or schema-only (not data-only)))
         (create-tables  (and create-tables create-ddl))
         (create-schemas (and create-schemas create-ddl))
         ;; foreign keys has a special meaning in data-only mode
         (foreign-keys   (if (eq :redshift (pgconn-variant (target-db copy)))
                             nil
                             foreign-keys))
         (drop-indexes   (if (eq :redshift (pgconn-variant (target-db copy)))
                             nil
                             (or reindex
                                 (and include-drop create-ddl))))
         (create-indexes (if (eq :redshift (pgconn-variant (target-db copy)))
                             nil
                             (or reindex
                                 (and create-indexes drop-indexes create-ddl))))

         (reset-sequences (if (eq :redshift (pgconn-variant (target-db copy)))
                              nil
                              reset-sequences))

         (*preserve-index-names*
          (or (eq :preserve index-names)
              ;; if we didn't create the tables, we are re-installing the
              ;; pre-existing indexes
              (not create-tables)))

         (copy-kernel  (make-kernel worker-count))
         (copy-channel (let ((lp:*kernel* copy-kernel)) (lp:make-channel)))
         (catalog      (handler-case
                           (fetch-metadata
                            copy
                            (make-catalog
                             :name (typecase (source-db copy)
                                     (db-connection
                                      (db-name (source-db copy)))
                                     (fd-connection
                                      (pathname-name
                                       (fd-path (source-db copy))))))
                            :materialize-views materialize-views
                            :create-indexes create-indexes
                            :foreign-keys foreign-keys
                            :including including
                            :excluding excluding)
                         (mssql::mssql-error (e)
                           (log-message :error "MSSQL ERROR: ~a" e)
                           (log-message :log "You might need to review the FreeTDS protocol version in your freetds.conf file, see http://www.freetds.org/userguide/choosingtdsprotocol.htm")
                           (return-from copy-database))
                         #+pgloader-image
                         (condition (e)
                           (log-message :error
                                        "~a: ~a"
                                        (conn-type (source-db copy))
                                        e)
                           (return-from copy-database))))
         pkeys
         (writers-count (make-hash-table :size (count-tables catalog)))
         (max-indexes   (when create-indexes
                          (max-indexes-per-table catalog)))
         (idx-kernel    (when (and max-indexes (< 0 max-indexes))
                          (make-kernel (or max-parallel-create-index
                                           max-indexes))))
         (idx-channel   (when idx-kernel
                          (let ((lp:*kernel* idx-kernel))
                            (lp:make-channel))))

         (task-count    0))

    ;; apply catalog level transformations to support the database migration
    ;; that's CAST rules, index WHERE clause rewriting and ALTER commands
    (handler-case
        (process-catalog copy catalog
                         :alter-table alter-table
                         :alter-schema alter-schema
                         :distribute distribute)

      #+pgloader-image
      ((or citus-rule-table-not-found citus-rule-is-missing-from-list) (e)
        (log-message :fatal "~a" e)
        (return-from copy-database))

      #+pgloader-image
      (condition (e)
        (log-message :fatal "Failed to process catalogs: ~a" e)
        (return-from copy-database)))

    ;; if asked, first drop/create the tables on the PostgreSQL side
    (handler-case
        (progn
          (prepare-pgsql-database copy
                                  catalog
                                  :truncate truncate
                                  :create-tables create-tables
                                  :create-schemas create-schemas
                                  :drop-indexes drop-indexes
                                  :drop-schema drop-schema
                                  :include-drop include-drop
                                  :foreign-keys foreign-keys
                                  :set-table-oids set-table-oids
                                  :materialize-views materialize-views)

          ;; if there's an AFTER SCHEMA DO/EXECUTE command, now is the time
          ;; to run it.
          (when after-schema
            (pgloader.parser::execute-sql-code-block (target-db copy)
                                                     :pre
                                                     after-schema
                                                     "after schema")))
      ;;
      ;; In case some error happens in the preparatory transaction, we
      ;; need to stop now and refrain from trying to load the data into
      ;; an incomplete schema.
      ;;
      (cl-postgres:database-error (e)
        (declare (ignore e))		; a log has already been printed
        (log-message :fatal "Failed to create the schema, see above.")

        ;; we might have some cleanup to do...
        (cleanup copy catalog :materialize-views materialize-views)

        (return-from copy-database)))

    (loop
       :for table :in (optimize-table-copy-ordering catalog)

       :do (let ((table-source (instanciate-table-copy-object copy table)))
             ;; first COPY the data from source to PostgreSQL, using copy-kernel
             (if (not copy-data)
                 ;; start indexing straight away then
                 (when create-indexes
                   (alexandria:appendf
                    pkeys
                    (create-indexes-in-kernel (target-db copy)
                                              table
                                              idx-kernel
                                              idx-channel)))

                 ;; prepare the writers-count hash-table, as we start
                 ;; copy-from, we have concurrency tasks writing.
                 (progn                 ; when copy-data
                   (setf (gethash table writers-count) concurrency)

                   (incf task-count
                         (copy-from table-source
                                    :concurrency concurrency
                                    :multiple-readers multiple-readers
                                    :kernel copy-kernel
                                    :channel copy-channel
                                    :on-error-stop on-error-stop
                                    :disable-triggers disable-triggers))))))

    ;; now end the kernels
    ;; and each time a table is done, launch its indexing
    (when copy-data
      (let ((lp:*kernel* copy-kernel))
        (with-stats-collection ("COPY Threads Completion" :section :post
                                                          :use-result-as-read t
                                                          :use-result-as-rows t)
          (loop :repeat task-count
             :do (destructuring-bind (task table seconds)
                     (lp:receive-result copy-channel)
                   (log-message :debug
                                "Finished processing ~a for ~s ~50T~6$s"
                                task (format-table-name table) seconds)
                   (when (eq :writer task)
                     ;;
                     ;; Start the CREATE INDEX parallel tasks only when
                     ;; the data has been fully copied over to the
                     ;; corresponding table, that's when the writers
                     ;; count is down to zero.
                     ;;
                     (decf (gethash table writers-count))
                     (log-message :debug "writers-counts[~a] = ~a"
                                  (format-table-name table)
                                  (gethash table writers-count))

                     (when (and create-indexes
                                (zerop (gethash table writers-count)))

                       (let* ((stats   pgloader.monitor::*sections*)
                              (section (get-state-section stats :data))
                              (table-stats (pgstate-get-label section table))
                              (pprint-secs
                               (pgloader.state::format-interval seconds nil)))
                         ;; in CCL we have access to the *sections* dynamic
                         ;; binding from another thread, in SBCL we access
                         ;; an empty copy.
                         (log-message :notice
                                      "DONE copying ~a in ~a~@[ for ~d rows~]"
                                     (format-table-name table)
                                     pprint-secs
                                     (when table-stats
                                       (pgtable-rows table-stats))))
                       (alexandria:appendf
                        pkeys
                        (create-indexes-in-kernel (target-db copy)
                                                  table
                                                  idx-kernel
                                                  idx-channel)))))
             :finally (progn
                        (lp:end-kernel :wait nil)
                        (return worker-count))))))

    (log-message :info "Done with COPYing data, waiting for indexes")

    (when create-indexes
      (let ((lp:*kernel* idx-kernel))
        ;; wait until the indexes are done being built...
        ;; don't forget accounting for that waiting time.
        (with-stats-collection ("Index Build Completion" :section :post
                                                         :use-result-as-read t
                                                         :use-result-as-rows t)
          (loop :for count :below (count-indexes catalog)
             :do (lp:receive-result idx-channel))
          (lp:end-kernel :wait t)
          (log-message :info "Done waiting for indexes")
          (count-indexes catalog))))

    ;;
    ;; Complete the PostgreSQL database before handing over.
    ;;
    (complete-pgsql-database copy
                             catalog
                             pkeys
                             :foreign-keys foreign-keys
                             :create-indexes create-indexes
                             ;; only create triggers (for default values)
                             ;; when we've been responsible for creating the
                             ;; tables -- otherwise assume the schema is
                             ;; good as it is
                             :create-triggers create-tables
                             :reset-sequences reset-sequences)

    ;;
    ;; Time to cleanup!
    ;;
    (cleanup copy catalog :materialize-views materialize-views)))

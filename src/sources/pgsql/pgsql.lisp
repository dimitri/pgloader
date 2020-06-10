;;;
;;; Read from a PostgreSQL database.
;;;

(in-package :pgloader.source.pgsql)

(defclass copy-pgsql (db-copy) ()
  (:documentation "pgloader PostgreSQL Data Source"))

(defmethod map-rows ((pgsql copy-pgsql) &key process-row-fn)
  "Extract PostgreSQL data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row"
  (let ((map-reader
         ;;
         ;; Build a Postmodern row reader that prepares a vector of strings
         ;; and call PROCESS-ROW-FN with the vector as single argument.
         ;;
         (cl-postgres:row-reader (fields)
           (let ((nb-cols (length fields)))
             (loop :while (cl-postgres:next-row)
                :do (let ((row (make-array nb-cols)))
                      (loop :for i :from 0
                         :for field :across fields
                         :do (setf (aref row i)
                                   (cl-postgres:next-field field)))
                      (funcall process-row-fn row)))))))

    (with-pgsql-connection ((source-db pgsql))
      (if (citus-backfill-table-p (target pgsql))
          ;;
          ;; SELECT dist_key, * FROM source JOIN dist ON ...
          ;;
          (let ((sql (citus-format-sql-select (source pgsql) (target pgsql))))
            (log-message :sql "~a" sql)
            (cl-postgres:exec-query pomo:*database* sql map-reader))

          ;;
          ;; No JOIN to add to backfill data in the SQL query here.
          ;;
          (let* ((cols   (mapcar #'column-name (fields pgsql)))
                 (sql
                  (format nil
                          "SELECT ~{~s::text~^, ~} FROM ~s.~s"
                          cols
                          (schema-source-name (table-schema (source pgsql)))
                          (table-source-name (source pgsql)))))
            (log-message :sql "~a" sql)
            (cl-postgres:exec-query pomo:*database* sql map-reader))))))

(defmethod copy-column-list ((pgsql copy-pgsql))
  "We are sending the data in the source columns ordering here."
  (mapcar (lambda (field) (ensure-quoted (column-name field)))
          (fields pgsql)))

(defmethod fetch-metadata ((pgsql copy-pgsql)
                           (catalog catalog)
                           &key
                             materialize-views
                             only-tables
                             create-indexes
                             foreign-keys
                             including
                             excluding)
  "PostgreSQL introspection to prepare the migration."
  (declare (ignore only-tables))
  (with-stats-collection ("fetch meta data"
                          :use-result-as-rows t
                          :use-result-as-read t
                          :section :pre)
    (with-pgsql-transaction (:pgconn (source-db pgsql))
      (let ((variant   (pgconn-variant (source-db pgsql)))
            (pgversion (pgconn-major-version (source-db pgsql))))
        ;;
        ;; First, create the source views that we're going to materialize in
        ;; the target database.
        ;;
        (when (and materialize-views (not (eq :all materialize-views)))
          (create-matviews materialize-views pgsql))

        (when (eq :pgdg variant)
          (list-all-sqltypes catalog
                             :including including
                             :excluding excluding))

        (list-all-columns catalog
                          :including including
                          :excluding excluding)

        (let* ((view-names (unless (eq :all materialize-views)
                             (mapcar #'matview-source-name materialize-views)))
               (including  (make-including-expr-from-view-names view-names)))
          (cond (view-names
                 (list-all-columns catalog
                                   :including including
                                   :table-type :view))

                ((eq :all materialize-views)
                 (list-all-columns catalog :table-type :view))))

        (when create-indexes
          (list-all-indexes catalog
                            :including including
                            :excluding excluding
                            :pgversion pgversion))

        (when (and (eq :pgdg variant) foreign-keys)
          (list-all-fkeys catalog
                          :including including
                          :excluding excluding))

        ;; return how many objects we're going to deal with in total
        ;; for stats collection
        (+ (count-tables catalog)
           (count-views catalog)
           (count-indexes catalog)
           (count-fkeys catalog)))))

  ;; be sure to return the catalog itself
  catalog)


(defmethod cleanup ((pgsql copy-pgsql) (catalog catalog) &key materialize-views)
  "When there is a PostgreSQL error at prepare-pgsql-database step, we might
   need to clean-up any view created in the source PostgreSQL connection for
   the migration purpose."
  (when materialize-views
    (with-pgsql-transaction (:pgconn  (source-db pgsql))
      (drop-matviews materialize-views pgsql))))

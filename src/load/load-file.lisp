;;;
;;; Generic API for pgloader sources
;;; Methods for source types with multiple files input
;;;

(in-package :pgloader.load)

(defmethod copy-database ((copy md-copy)
                          &key
                            (on-error-stop *on-error-stop*)
                            truncate
                            disable-triggers
			    drop-indexes

                            max-parallel-create-index

                            ;; generic API, but ignored here
                            (worker-count 4)
                            (concurrency 1)

                            data-only
			    schema-only
                            create-tables
			    include-drop
                            foreign-keys
			    create-indexes
			    reset-sequences
                            materialize-views
                            set-table-oids
                            including
                            excluding)
  "Copy the contents of the COPY formated file to PostgreSQL."
  (declare (ignore data-only schema-only
                   create-tables include-drop foreign-keys
                   create-indexes reset-sequences materialize-views
                   set-table-oids including excluding))

  (let* ((*on-error-stop* on-error-stop)
         (pgconn (target-db copy))
         pgsql-catalog)

    (handler-case
        (with-pgsql-connection (pgconn)
          (setf pgsql-catalog
                (fetch-pgsql-catalog (db-name pgconn)
                                     :table (target copy)
                                     :variant (pgconn-variant pgconn)
                                     :pgversion (pgconn-major-version pgconn)))

          ;; if the user didn't tell us the column list of the table, now is
          ;; a proper time to set it in the copy object
          (unless (and (slot-boundp copy 'columns)
                       (slot-value copy 'columns))
            (setf (columns copy)
                  (mapcar (lambda (col)
                            ;; we need to handle the md-copy format for the
                            ;; column list, which allow for user given
                            ;; options: each column is a list which car is
                            ;; the column name.
                            (list (column-name col)))
                          (table-field-list (first (table-list pgsql-catalog))))))

          (log-message :data "CATALOG: ~s" pgsql-catalog)

          ;; this sets (table-index-list (target copy))
          (maybe-drop-indexes pgsql-catalog :drop-indexes drop-indexes)

          ;; now is the proper time to truncate, before parallel operations
          (when truncate
            (truncate-tables pgsql-catalog)))

      (cl-postgres:database-error (e)
        (log-message :fatal "Failed to prepare target PostgreSQL table.")
        (log-message :fatal "~a" e)
        (return-from copy-database)))

    ;; Keep the PostgreSQL table target around in the copy instance,
    ;; with the following subtleties to deal with:
    ;;   1. the catalog fetching did fill-in PostgreSQL columns as fields
    ;;   2. we might target fewer pg columns than the table actually has
    (let ((table (first (table-list pgsql-catalog))))
      (setf (table-column-list table)
            (loop :for column-name :in (mapcar #'first (columns copy))
               :collect (find column-name (table-field-list table)
                              :key #'column-name
                              :test #'string=)))
      (setf (target copy) table))

    ;; expand the specs of our source, we might have to care about several
    ;; files actually.
    (let* ((lp:*kernel* (make-kernel worker-count))
           (channel     (lp:make-channel))
           (path-list   (expand-spec (source copy)))
           (task-count  0))
      (with-stats-collection ("Files Processed" :section :post
                                                :use-result-as-read t
                                                :use-result-as-rows t)
        (loop :for path-spec :in path-list
           :count t
           :do (let ((table-source (clone-copy-for copy path-spec)))
                 (when (and (header table-source) (null (fields table-source)))
                   (parse-header table-source))
                 (incf task-count
                       (copy-from table-source
                                  :concurrency concurrency
                                  :kernel lp:*kernel*
                                  :channel channel
                                  :on-error-stop on-error-stop
                                  :disable-triggers disable-triggers)))))

      ;; end kernel
      (with-stats-collection ("COPY Threads Completion" :section :post
                                                        :use-result-as-read t
                                                        :use-result-as-rows t)
        (loop :repeat task-count
           :do (handler-case
                   (destructuring-bind (task table seconds)
                       (lp:receive-result channel)
                     (log-message :debug
                                  "Finished processing ~a for ~s ~50T~6$s"
                                  task (format-table-name table) seconds))
                 (condition (e)
                   (log-message :fatal "~a" e)))
           :finally (progn
                      (lp:end-kernel :wait nil)
                      (return task-count))))
      (lp:end-kernel :wait t))

    ;; re-create the indexes from the target table entry
    (create-indexes-again (target-db copy)
                          pgsql-catalog
                          :max-parallel-create-index max-parallel-create-index
                          :drop-indexes drop-indexes)))

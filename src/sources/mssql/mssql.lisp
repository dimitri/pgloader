;;;
;;; Tools to handle the MS SQL Database
;;;

(in-package :pgloader.source.mssql)

(defmethod map-rows ((mssql copy-mssql) &key process-row-fn)
  "Extract Mssql data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (with-connection (*mssql-db* (source-db mssql))
    (let* ((sql  (format nil "SELECT ~{~a~^, ~} FROM [~a].[~a];"
                         (get-column-list mssql)
                         (schema-source-name (table-schema (source mssql)))
                         (table-source-name (source mssql)))))
      (log-message :debug "~a" sql)
      (handler-bind
          ((babel-encodings:end-of-input-in-character
            #'(lambda (c)
                (update-stats :data (target mssql) :errs 1)
                (log-message :error "~a" c)
                (invoke-restart 'mssql::use-nil)))

           (babel-encodings:character-decoding-error
            #'(lambda (c)
                (update-stats :data (target mssql) :errs 1)
                (let ((encoding
                       (babel-encodings:character-coding-error-encoding c))
                      (position
                       (babel-encodings:character-coding-error-position c))
                      (character
                       (aref (babel-encodings:character-coding-error-buffer c)
                             (babel-encodings:character-coding-error-position c))))
                  (log-message :error
                               "~a: Illegal ~a character starting at position ~a: ~a."
                               (table-schema (source mssql))
                               encoding
                               position
                               character))
                (invoke-restart 'mssql::use-nil))))
        (mssql::map-query-results sql
                                  :row-fn process-row-fn
                                  :connection (conn-handle *mssql-db*))))))

(defmethod copy-column-list ((mssql copy-mssql))
  "We are sending the data in the MS SQL columns ordering here."
  (mapcar #'apply-identifier-case (mapcar #'mssql-column-name (fields mssql))))

(defmethod fetch-metadata ((mssql copy-mssql)
                           (catalog catalog)
                           &key
                             materialize-views
                             create-indexes
                             foreign-keys
                             including
                             excluding)
  "MS SQL introspection to prepare the migration."
  (with-stats-collection ("fetch meta data"
                          :use-result-as-rows t
                          :use-result-as-read t
                          :section :pre)
    (with-connection (*mssql-db* (source-db mssql))
      ;; If asked to MATERIALIZE VIEWS, now is the time to create them in MS
      ;; SQL, when given definitions rather than existing view names.
      (when (and materialize-views (not (eq :all materialize-views)))
        (create-matviews materialize-views mssql))

      (fetch-columns catalog mssql
                     :including including
                     :excluding excluding)

      ;; fetch view (and their columns) metadata, covering comments too
      (let* ((view-names (unless (eq :all materialize-views)
                           (mapcar #'matview-source-name materialize-views)))
             (including
              (loop :for (schema-name . view-name) :in view-names
                 :do (let* ((schema-name (or schema-name "dbo"))
                            (schema-entry
                             (or (assoc schema-name including :test #'string=)
                                 (progn (push (cons schema-name nil) including)
                                        (assoc schema-name including
                                               :test #'string=)))))
                       (push-to-end view-name (cdr schema-entry))))))
        (cond (view-names
               (fetch-columns catalog mssql
                              :including including
                              :excluding excluding
                              :table-type :view))

              ((eq :all materialize-views)
               (fetch-columns catalog mssql :table-type :view))))

      (when create-indexes
        (fetch-indexes catalog mssql
                       :including including
                       :excluding excluding))

      (when foreign-keys
        (fetch-foreign-keys catalog mssql
                            :including including
                            :excluding excluding))

      ;; return how many objects we're going to deal with in total
      ;; for stats collection
      (+ (count-tables catalog)
         (count-views catalog)
         (count-indexes catalog)
         (count-fkeys catalog))))

  ;; be sure to return the catalog itself
  catalog)


(defmethod cleanup ((mssql copy-mssql) (catalog catalog) &key materialize-views)
  "When there is a PostgreSQL error at prepare-pgsql-database step, we might
   need to clean-up any view created in the MS SQL connection for the
   migration purpose."
  (when materialize-views
    (with-connection (*mssql-db* (source-db mssql))
      (drop-matviews materialize-views mssql))))

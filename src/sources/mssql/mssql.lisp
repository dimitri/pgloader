;;;
;;; Tools to handle the MS SQL Database
;;;

(in-package :pgloader.source.mssql)

(defclass copy-mssql (db-copy)
  ((encoding :accessor encoding         ; allows forcing encoding
             :initarg :encoding
             :initform nil))
  (:documentation "pgloader MS SQL Data Source"))

(defmethod initialize-instance :after ((source copy-mssql) &key)
  "Add a default value for transforms in case it's not been provided."
  (let* ((transforms (when (slot-boundp source 'transforms)
		       (slot-value source 'transforms))))
    (when (and (slot-boundp source 'fields) (slot-value source 'fields))
      ;; cast typically happens in copy-database in the schema structure,
      ;; and the result is then copied into the copy-mysql instance.
      (unless (and (slot-boundp source 'columns) (slot-value source 'columns))
        (setf (slot-value source 'columns)
              (mapcar #'cast (slot-value source 'fields))))

      (unless transforms
        (setf (slot-value source 'transforms)
              (mapcar #'column-transform (slot-value source 'columns)))))))

(defmethod map-rows ((mssql copy-mssql) &key process-row-fn)
  "Extract Mssql data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (with-connection (*mssql-db* (source-db mssql))
    (let* ((sql  (format nil "SELECT ~{~a~^, ~} FROM [~a].[~a];"
                         (get-column-list (fields mssql))
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

(defmethod fetch-metadata ((mssql copy-mssql)
                           (catalog catalog)
                           &key
                             materialize-views
                             only-tables
                             create-indexes
                             foreign-keys
                             including
                             excluding)
  "MS SQL introspection to prepare the migration."
  (declare (ignore materialize-views only-tables))
  (with-stats-collection ("fetch meta data"
                          :use-result-as-rows t
                          :use-result-as-read t
                          :section :pre)
      (with-connection (*mssql-db* (source-db mssql))
        (list-all-columns catalog
                          :including including
                          :excluding excluding)

        (when create-indexes
          (list-all-indexes catalog
                            :including including
                            :excluding excluding))

        (when foreign-keys
          (list-all-fkeys catalog
                          :including including
                          :excluding excluding))

        ;; return how many objects we're going to deal with in total
        ;; for stats collection
        (+ (count-tables catalog) (count-indexes catalog))))

  ;; be sure to return the catalog itself
  catalog)


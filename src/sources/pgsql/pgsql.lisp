;;;
;;; Read from a PostgreSQL database.
;;;

(in-package :pgloader.source.pgsql)

(defclass copy-pgsql (db-copy) ()
  (:documentation "pgloader PostgreSQL Data Source"))

(defmethod initialize-instance :after ((source copy-pgsql) &key)
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
      (let* ((cols   (mapcar #'column-name (fields pgsql)))
             (sql
              (format nil "SELECT ~{~s::text~^, ~} FROM ~s.~s" cols
                      (schema-source-name (table-schema (source pgsql)))
                      (table-source-name (source pgsql)))))
        (cl-postgres:exec-query pomo:*database* sql map-reader)))))

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
  (declare (ignore materialize-views only-tables))
  (with-stats-collection ("fetch meta data"
                          :use-result-as-rows t
                          :use-result-as-read t
                          :section :pre)
    (with-pgsql-transaction (:pgconn (source-db pgsql))
      (let ((variant (pgconn-variant (source-db pgsql))))
       (when (eq :pgdg variant)
         (list-all-sqltypes catalog
                            :including including
                            :excluding excluding))

       (list-all-columns catalog
                         :including including
                         :excluding excluding)

       (when create-indexes
         (list-all-indexes catalog
                           :including including
                           :excluding excluding))

       (when (and (eq :pgdg variant) foreign-keys)
         (list-all-fkeys catalog
                         :including including
                         :excluding excluding))

       ;; return how many objects we're going to deal with in total
       ;; for stats collection
       (+ (count-tables catalog) (count-indexes catalog)))))

  ;; be sure to return the catalog itself
  catalog)

;;;
;;; Tools to handle the DBF file format
;;;

(in-package :pgloader.source.db3)

(defmethod map-rows ((copy-db3 copy-db3) &key process-row-fn)
  "Extract DB3 data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (with-connection (conn (source-db copy-db3))
    (let ((stream (conn-handle (source-db copy-db3)))
          (db3    (fd-db3 (source-db copy-db3))))

      ;; when the pgloader command has an ENCODING clause, it takes
      ;; precedence to the encoding embedded in the db3 file, if any.
      (when (and (encoding copy-db3)
                 (db3::encoding db3)
                 (not (eq (encoding copy-db3) (db3::encoding db3))))
        (log-message :warning "Forcing encoding to ~a, db3 file has ~a"
                     (encoding copy-db3) (db3::encoding db3))
        (setf (db3::encoding db3) (encoding copy-db3)))

      (loop
         :with count := (db3:record-count db3)
         :repeat count
         :for (row-array deleted) := (multiple-value-list
                                      (db3:load-record db3 stream))
         :unless deleted
         :do (funcall process-row-fn row-array)
         :finally (return count)))))

(defmethod instanciate-table-copy-object ((db3 copy-db3) (table table))
  "Create an new instance for copying TABLE data."
  (let ((new-instance (change-class (call-next-method db3 table) 'copy-db3)))
    (setf (encoding new-instance) (encoding db3))
    new-instance))

(defmethod fetch-metadata ((db3 copy-db3) (catalog catalog)
                           &key
                             materialize-views
                             only-tables
                             create-indexes
                             foreign-keys
                             including
                             excluding)
  "Collect DB3 metadata and prepare our catalog from that."
  (declare (ignore materialize-views only-tables create-indexes foreign-keys
                   including excluding))
  (let* ((table  (or (target db3) (source db3)))
         (schema (or (when (table-schema table)
                       (push-to-end (table-schema table)
                                    (catalog-schema-list catalog)))
                     (add-schema catalog (table-name table)))))
    (push-to-end table (schema-table-list schema))

    (with-connection (conn (source-db db3))
      (fetch-columns table db3))

    catalog))


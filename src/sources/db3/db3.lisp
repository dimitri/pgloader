;;;
;;; Tools to handle the DBF file format
;;;

(in-package :pgloader.db3)

;;;
;;; Integration with pgloader
;;;
(defclass copy-db3 (db-copy)
  ((encoding    :accessor encoding	  ; file encoding
	        :initarg :encoding))
  (:documentation "pgloader DBF Data Source"))

(defmethod initialize-instance :after ((db3 copy-db3) &key)
  "Add a default value for transforms in case it's not been provided."
  (setf (slot-value db3 'source)
        (let ((table-name (pathname-name (fd-path (source-db db3)))))
          (make-table :source-name table-name
                      :name (apply-identifier-case table-name)))))

(defmethod map-rows ((copy-db3 copy-db3) &key process-row-fn)
  "Extract DB3 data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (with-connection (conn (source-db copy-db3))
    (let ((stream (conn-handle (source-db copy-db3)))
          (db3    (fd-db3 (source-db copy-db3)))
          (db3:*external-format* (encoding copy-db3)))
      (loop
         :with count := (db3:record-count db3)
         :repeat count
         :for row-array := (db3:load-record db3 stream)
         :do (funcall process-row-fn row-array)
         :finally (return count)))))

(defun fetch-db3-metadata (db3 table)
  "Collect DB3 metadata and prepare our catalog from that."
  (with-connection (conn (source-db db3))
    (list-all-columns (fd-db3 conn) table)))

(defmethod copy-database ((db3 copy-db3)
                          &key
                            table
			    data-only
			    schema-only
                            (truncate         t)
                            (disable-triggers nil)
                            (create-tables    t)
			    (include-drop     t)
			    (create-indexes   t)
			    (reset-sequences  t))
  "Open the DB3 and stream its content to a PostgreSQL database."
  (declare (ignore create-indexes reset-sequences))
  (let* ((table  (or table (target db3) (source db3)))
         (schema (make-schema :name (table-name table)
                              :table-list (list table))))

    ;; fix the table-name in the db3 object
    (setf (target db3) table)

    ;; Get the db3 metadata and cast the db3 schema to PostgreSQL
    (fetch-db3-metadata db3 table)
    (cast table)

    (handler-case
        (when (and (or create-tables schema-only) (not data-only))
          (prepare-pgsql-database db3
                                  (make-catalog :name (table-name table)
                                                :schema-list (list schema))
                                  :include-drop include-drop))

      (cl-postgres::database-error (e)
        (declare (ignore e))            ; a log has already been printed
        (log-message :fatal "Failed to create the schema, see above.")
        (return-from copy-database)))

    (unless schema-only
      (setf (fields db3)     (table-field-list table)
            (columns db3)    (table-column-list table)
            (transforms db3) (mapcar #'column-transform
                                     (table-column-list table)))
      (copy-from db3 :truncate truncate :disable-triggers disable-triggers))))

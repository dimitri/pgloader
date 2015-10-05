;;;
;;; Tools to handle the DBF file format
;;;

(in-package :pgloader.db3)

;;;
;;; Integration with pgloader
;;;
(defclass copy-db3 (copy)
  ((encoding    :accessor encoding	  ; file encoding
	        :initarg :encoding))
  (:documentation "pgloader DBF Data Source"))

(defmethod initialize-instance :after ((db3 copy-db3) &key)
  "Add a default value for transforms in case it's not been provided."
  (setf (slot-value db3 'source) (pathname-name (fd-path (source-db db3))))

  (with-connection (conn (source-db db3))
    (unless (and (slot-boundp db3 'columns) (slot-value db3 'columns))
      (setf (slot-value db3 'columns)
            (list-all-columns (fd-db3 conn)
                              (or (typecase (target db3)
                                    (cons   (cdr (target db3)))
                                    (string (target db3)))
                                  (source db3)))))

    (let ((transforms (when (slot-boundp db3 'transforms)
                        (slot-value db3 'transforms))))
      (unless transforms
        (setf (slot-value db3 'transforms)
              (list-transforms (fd-db3 conn)))))))

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

(defmethod copy-database ((db3 copy-db3)
                          &key
                            table-name
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
  (let* ((table-name  (or table-name
			  (target db3)
			  (source db3))))

    ;; fix the table-name in the db3 object
    (setf (target db3) table-name)

    (handler-case
        (when (and (or create-tables schema-only) (not data-only))
          (with-stats-collection ("create, truncate" :section :pre)
            (with-pgsql-transaction (:pgconn (target-db db3))
              (when create-tables
                (with-schema (tname table-name)
                  (log-message :notice "Create table \"~a\"" tname)
                  (create-tables (columns db3)
                                 :include-drop include-drop
                                 :if-not-exists t))))))

      (cl-postgres::database-error (e)
        (declare (ignore e))            ; a log has already been printed
        (log-message :fatal "Failed to create the schema, see above.")
        (return-from copy-database)))

    (unless schema-only
      (copy-from db3 :truncate truncate :disable-triggers disable-triggers))))

;;;
;;; Tools to handle IBM PC version of IXF file format
;;;
;;; http://www-01.ibm.com/support/knowledgecenter/SSEPGG_10.5.0/com.ibm.db2.luw.admin.dm.doc/doc/r0004667.html

(in-package :pgloader.ixf)

;;;
;;; Integration with pgloader
;;;
(defclass copy-ixf (db-copy)
  ((timezone    :accessor timezone	  ; timezone
	        :initarg :timezone
                :initform local-time:+utc-zone+))
  (:documentation "pgloader IXF Data Source"))

(defmethod initialize-instance :after ((source copy-ixf) &key)
  "Add a default value for transforms in case it's not been provided."
  (setf (slot-value source 'source)
        (let ((table-name (pathname-name (fd-path (source-db source)))))
          (make-table :source-name table-name
                      :name (apply-identifier-case table-name))))

  ;; force default timezone when nil
  (when (null (timezone source))
    (setf (timezone source) local-time:+utc-zone+)))

(defmethod map-rows ((copy-ixf copy-ixf) &key process-row-fn)
  "Extract IXF data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (let ((local-time:*default-timezone* (timezone copy-ixf)))
    (log-message :notice "Parsing IXF with TimeZone: ~a"
                 (local-time::timezone-name local-time:*default-timezone*))
    (with-connection (conn (source-db copy-ixf))
      (let ((ixf    (ixf:make-ixf-file :stream (conn-handle conn))))
        (ixf:read-headers ixf)
        (ixf:map-data ixf process-row-fn)))))

(defun fetch-ixf-metadata (ixf table)
  "Collect IXF metadata and prepare our catalog from that."
  (with-connection (conn (source-db ixf))
    (list-all-columns (conn-handle conn) table)))

(defmethod copy-database ((ixf copy-ixf)
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
  "Open the IXF and stream its content to a PostgreSQL database."
  (declare (ignore create-indexes reset-sequences))
  (let* ((table  (or table (target ixf) (source ixf)))
         (schema (make-schema :name (table-name table)
                              :table-list (list table))))

    ;; fix the table in the ixf object
    (setf (target ixf) table)

    ;; Get the IXF metadata and cast the IXF schema to PostgreSQL
    (fetch-ixf-metadata ixf table)
    (cast table)

    (handler-case
        (when (and (or create-tables schema-only) (not data-only))
          (prepare-pgsql-database ixf
                                  (make-catalog :name (table-name table)
                                                :schema-list (list schema))
                                  :include-drop include-drop))

      (cl-postgres::database-error (e)
        (declare (ignore e))            ; a log has already been printed
        (log-message :fatal "Failed to create the schema, see above.")
        (return-from copy-database)))

    (unless schema-only
      (setf (fields ixf)     (table-field-list table)
            (columns ixf)    (table-column-list table)
            (transforms ixf) (mapcar #'column-transform
                                     (table-column-list table)))
      (copy-from ixf :truncate truncate :disable-triggers disable-triggers))))


;;;
;;; Tools to handle IBM PC version of IXF file format
;;;
;;; http://www-01.ibm.com/support/knowledgecenter/SSEPGG_10.5.0/com.ibm.db2.luw.admin.dm.doc/doc/r0004667.html

(in-package :pgloader.source.ixf)

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

(defmethod fetch-columns ((table table) (ixf copy-ixf)
                          &key &allow-other-keys
                          &aux (ixf-stream (conn-handle (source-db ixf))))
  "Return the list of columns for the given IXF-FILE-NAME."
  (ixf:with-ixf-stream (ixf ixf-stream)
    (loop :for field :across (ixf:ixf-table-columns (ixf:ixf-file-table ixf))
       :do (add-field table field))))

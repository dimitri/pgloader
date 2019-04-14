;;;
;;; Tools to handle the DBF file format
;;;

(in-package :pgloader.source.db3)

(defclass dbf-connection (fd-connection)
  ((db3 :initarg db3 :accessor fd-db3))
  (:documentation "pgloader connection parameters for DBF files."))

(defmethod initialize-instance :after ((dbfconn dbf-connection) &key)
  "Assign the type slot to dbf."
  (setf (slot-value dbfconn 'type) "dbf"))

(defmethod open-connection ((dbfconn dbf-connection) &key)
  (setf (conn-handle dbfconn)
        (open (fd-path dbfconn)
              :direction :input
              :element-type '(unsigned-byte 8)))
  (let ((db3 (make-instance 'db3:db3 :filename  (fd-path dbfconn))))
    (db3:load-header db3 (conn-handle dbfconn))
    (setf (fd-db3 dbfconn) db3))
  dbfconn)

(defmethod close-connection ((dbfconn dbf-connection))
  (db3:close-memo (fd-db3 dbfconn))
  (close (conn-handle dbfconn))
  (setf (conn-handle dbfconn) nil
        (fd-db3 dbfconn) nil)
  dbfconn)

(defmethod clone-connection ((c dbf-connection))
  (let ((clone (change-class (call-next-method c) 'dbf-connection)))
    (setf (fd-db3 clone) (fd-db3 c))
    clone))

(defun list-all-columns (db3 table)
  "Return the list of columns for the given DB3-FILE-NAME."
  (loop
     :for field :in (db3::fields db3)
     :do (add-field table (make-db3-field (db3::field-name field)
                                          (string (db3::field-type field))
                                          (db3::field-length field)))))

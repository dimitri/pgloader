;;;
;;; Tools to handle IBM PC version of IXF file format
;;;

(in-package :pgloader.source.ixf)

(defclass ixf-connection (fd-connection) ()
  (:documentation "pgloader connection parameters for IXF files."))

(defmethod initialize-instance :after ((ixfconn ixf-connection) &key)
  "Assign the type slot to dbf."
  (setf (slot-value ixfconn 'type) "ixf"))

(defmethod open-connection ((ixfconn ixf-connection) &key)
  (setf (conn-handle ixfconn)
        (open (fd-path ixfconn)
              :direction :input
              :element-type '(unsigned-byte 8)))
  ixfconn)

(defmethod close-connection ((ixfconn ixf-connection))
  (close (conn-handle ixfconn))
  (setf (conn-handle ixfconn) nil)
  ixfconn)

(defmethod clone-connection ((c ixf-connection))
  (change-class (call-next-method c) 'ixf-connection))


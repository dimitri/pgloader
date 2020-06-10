;;;
;;; Tools to handle the DBF file format
;;;

(in-package :pgloader.source.db3)

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

(defmethod fetch-columns ((table table) (db3 copy-db3)
                          &key &allow-other-keys
                          &aux (dbfconn (fd-db3 (source-db db3))))
  "Return the list of columns for the given DB3-FILE-NAME."
  (loop
     :for field :in (db3::fields dbfconn)
     :do (add-field table (make-db3-coldef (db3::field-name field)
                                           (string (db3::field-type field))
                                           (db3::field-length field)))))

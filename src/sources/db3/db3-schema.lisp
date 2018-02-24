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
  (let ((db3 (make-instance 'db3:db3)))
    (db3:load-header db3 (conn-handle dbfconn))
    (setf (fd-db3 dbfconn) db3))
  dbfconn)

(defmethod close-connection ((dbfconn dbf-connection))
  (close (conn-handle dbfconn))
  (setf (conn-handle dbfconn) nil
        (fd-db3 dbfconn) nil)
  dbfconn)

(defmethod clone-connection ((c dbf-connection))
  (let ((clone (change-class (call-next-method c) 'dbf-connection)))
    (setf (fd-db3 clone) (fd-db3 c))
    clone))

(defvar *db3-pgsql-type-mapping*
  '(("C" . "text")			; ignore field-length
    ("N" . "numeric")			; handle both integers and floats
    ("L" . "boolean")			; PostgreSQL compatible representation
    ("D" . "date")			; no TimeZone in DB3 files
    ("M" . "text")))			; not handled yet

(defstruct (db3-field
	     (:constructor make-db3-field (name type length)))
  name type length)

(defun list-all-columns (db3 table)
  "Return the list of columns for the given DB3-FILE-NAME."
  (loop
     :for field :in (db3::fields db3)
     :do (add-field table (make-db3-field (db3::field-name field)
                                          (db3::field-type field)
                                          (db3::field-length field)))))

(defmethod cast ((field db3-field) &key &allow-other-keys)
  "Return the PostgreSQL type definition given the DB3 one."
  (let* ((type (db3-field-type field))
         (transform
          (cond ((string= type "C") #'db3-trim-string)
                ((string= type "N") #'db3-numeric-to-pgsql-numeric)
                ((string= type "L") #'logical-to-boolean)
                ((string= type "D") #'db3-date-to-pgsql-date)
                (t                  nil))))
    (make-column :name (apply-identifier-case (db3-field-name field))
                 :type-name (cdr (assoc type *db3-pgsql-type-mapping*
                                        :test #'string=))
                 :transform transform)))

(declaim (inline logical-to-boolean
		 db3-trim-string
		 db3-date-to-pgsql-date))

(defun logical-to-boolean (value)
  "Convert a DB3 logical value to a PostgreSQL boolean."
  (if (string= value "?") nil value))

(defun db3-trim-string (value)
  "DB3 Strings a right padded with spaces, fix that."
  (string-right-trim '(#\Space) value))

(defun db3-numeric-to-pgsql-numeric (value)
  "DB3 numerics should be good to go, but might contain spaces."
  (let ((trimmed-string (string-right-trim '(#\Space) value)))
    (unless (string= "" trimmed-string)
      trimmed-string)))

(defun db3-date-to-pgsql-date (value)
  "Convert a DB3 date to a PostgreSQL date."
  (when (and value (string/= "" value) (= 8 (length value)))
    (let ((year  (parse-integer (subseq value 0 4) :junk-allowed t))
          (month (parse-integer (subseq value 4 6) :junk-allowed t))
          (day   (parse-integer (subseq value 6 8) :junk-allowed t)))
      (when (and year month day)
        (format nil "~4,'0d-~2,'0d-~2,'0d" year month day)))))


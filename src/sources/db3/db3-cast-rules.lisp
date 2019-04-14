;;;
;;; Tools to handle MySQL data type casting rules
;;;

(in-package :pgloader.source.db3)

;;;
;;; The default DB3 Type Casting Rules
;;;
(defparameter *db3-default-cast-rules*
  `((:source (:type "C")
     :target (:type "text")
     :using db3-trim-string)

    (:source (:type "N")
     :target (:type "numeric")
     :using db3-numeric-to-pgsql-numeric)

    (:source (:type "L")
     :target (:type "boolean")
     :using logical-to-boolean)

    (:source (:type "D")
     :target (:type "date")
     :using db3-date-to-pgsql-date)

    (:source (:type "M")
     :target (:type "text")
     :using db3-trim-string))
  "Data Type Casting rules to migrate from DB3 to PostgreSQL")

(defstruct (db3-field
	     (:constructor make-db3-field (name type length)))
  name type length default (nullable t) extra)

(defmethod cast ((field db3-field) &key table)
  "Return the PostgreSQL type definition given the DB3 one."
  (let ((table-name (table-name table)))
    (with-slots (name type length default nullable extra) field
      (apply-casting-rules table-name name type type default nullable extra))))

;;;
;;; Transformation functions
;;;
(declaim (inline logical-to-boolean
		 db3-trim-string
                 db3-numeric-to-pgsql-numeric
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


;;;
;;; Tools to handle data conversion to PostgreSQL format
;;;
;;; Any function that you want to use to transform data with will get looked
;;; up in the pgloader.transforms package, when using the default USING
;;; syntax for transformations.

(in-package :pgloader.transforms)

(defun nil-to-cl-postgres-null (value)
  "cl-mysql returns nil for NULL and cl-postgres wants :NULL"
  (if (null value) :NULL value))

(defun zero-dates-to-null (date-string)
  "MySQL accepts '0000-00-00' as a date, we want :null instead."
  (cond
    ((null date-string) nil)
    ((string= date-string "") nil)
    ((string= date-string "0000-00-00") nil)
    ((string= date-string "0000-00-00 00:00:00") nil)
    (t date-string)))

(defun date-with-no-separator
    (date-string
     &optional (format '((:year     0  4)
			 (:month    4  6)
			 (:day      6  8)
			 (:hour     8 10)
			 (:minute  10 12)
			 (:seconds 12 14))))
  "Apply this function when input date in like '20041002152952'"
  ;; only process non-zero dates
  (if (null (transform-zero-dates-to-null date-string)) nil
      (destructuring-bind (&key year month day hour minute seconds
				&allow-other-keys)
	    (loop
	       for (name start end) in format
	       append (list name (subseq date-string start end)))
	(format nil "~a-~a-~a ~a:~a:~a" year month day hour minute seconds))))

(defun tinyint-to-boolean (integer-string)
  "When using MySQL, strange things will happen, like encoding booleans into
   tinyiny that are either 0 (false) or 1 (true). Of course PostgreSQL wants
   'f' and 't', respectively."
  (if (string= "0" integer-string) "f" "t"))

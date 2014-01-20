;;;
;;; Tools to handle data conversion to PostgreSQL format
;;;
;;; Any function that you want to use to transform data with will get looked
;;; up in the pgloader.transforms package, when using the default USING
;;; syntax for transformations.

(defpackage #:pgloader.transforms
  (:use #:cl)
  (:export #:intern-symbol))

(in-package :pgloader.transforms)

(declaim (inline intern-symbol
		 zero-dates-to-null
		 date-with-no-separator
		 time-with-no-separator
		 tinyint-to-boolean
		 int-to-ip
		 ip-range
		 convert-mysql-point
		 float-to-string
                 empty-string-to-null
		 set-to-enum-array
		 right-trim
		 byte-vector-to-bytea))


;;;
;;; Some tools for reading expressions in the parser, and evaluating them.
;;;
(defun intern-symbol (symbol-name)
  (intern (string-upcase symbol-name)
	  (find-package "PGLOADER.TRANSFORMS")))


;;;
;;; Transformation functions
;;;
(defun zero-dates-to-null (date-string)
  "MySQL accepts '0000-00-00' as a date, we want NIL (SQL NULL) instead."
  (cond
    ((null date-string) nil)
    ((string= date-string "") nil)
    ;; day is 00
    ((string= date-string "0000-00-00" :start1 8 :end1 10 :start2 8 ) nil)
    ;; month is 00
    ((string= date-string "0000-00-00" :start1 5 :end1 7 :start2 5 :end2 7) nil)
    ;; year is 0000
    ((string= date-string "0000-00-00" :start1 0 :end1 3 :start2 0 :end2 3) nil)
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
  (declare (type string date-string))
  (if (null (zero-dates-to-null date-string)) nil
      (destructuring-bind (&key year month day hour minute seconds
				&allow-other-keys)
	    (loop
	       for (name start end) in format
	       append (list name (subseq date-string start end)))
	(format nil "~a-~a-~a ~a:~a:~a" year month day hour minute seconds))))

(defun time-with-no-separator
    (time-string
     &optional (format '((:hour     0 2)
			 (:minute   2 4)
			 (:seconds  4 6)
			 (:msecs    6 nil))))
  "Apply this function when input date in like '08231560'"
  (declare (type string time-string))
  (destructuring-bind (&key hour minute seconds msecs
			    &allow-other-keys)
      (loop
	 for (name start end) in format
	 append (list name (subseq time-string start end)))
    (format nil "~a:~a:~a.~a" hour minute seconds msecs)))

(defun tinyint-to-boolean (integer-string)
  "When using MySQL, strange things will happen, like encoding booleans into
   tinyiny that are either 0 (false) or 1 (true). Of course PostgreSQL wants
   'f' and 't', respectively."
  (if (string= "0" integer-string) "f" "t"))

(defun int-to-ip (int)
  "Transform an IP as integer into its dotted notation, optimised code from
   stassats."
  (declare (optimize speed)
           (type (unsigned-byte 32) int))
  (let ((table (load-time-value
                (let ((vec (make-array (+ 1 #xFFFF))))
                  (loop for i to #xFFFF
		     do (setf (aref vec i)
			      (coerce (format nil "~a.~a"
					      (ldb (byte 8 8) i)
					      (ldb (byte 8 0) i))
				      'simple-base-string)))
                  vec)
                t)))
    (declare (type (simple-array simple-base-string (*)) table))
    (concatenate 'simple-base-string
		 (aref table (ldb (byte 16 16) int))
		 "."
		 (aref table (ldb (byte 16 0) int)))))

(defun ip-range (start-integer-string end-integer-string)
  "Transform a couple of integers to an IP4R ip range notation."
  (declare (optimize speed)
	   (type string start-integer-string end-integer-string))
  (let ((ip-start (int-to-ip (parse-integer start-integer-string)))
	(ip-end   (int-to-ip (parse-integer end-integer-string))))
    (concatenate 'simple-base-string ip-start "-" ip-end)))

(defun convert-mysql-point (mysql-point-as-string)
  "Transform the MYSQL-POINT-AS-STRING into a suitable representation for
   PostgreSQL.

  Input:   \"POINT(48.5513589 7.6926827)\" ; that's using astext(column)
  Output:  (48.5513589,7.6926827)"
  (when mysql-point-as-string
    (let* ((point (subseq mysql-point-as-string 5)))
      (setf (aref point (position #\Space point)) #\,)
      point)))

(defun float-to-string (float)
  "Transform a Common Lisp float value into its string representation as
   accepted by PostgreSQL, that is 100.0 rather than 100.0d0."
  (declare (type float float))
  (let ((*read-default-float-format* 'double-float))
    (princ-to-string float)))

(defun set-to-enum-array (set-string)
  "Transform a MySQL SET value into a PostgreSQL ENUM Array"
  (format nil "{~a}" set-string))

(defun empty-string-to-null (string)
  "MySQL ENUM sometimes return an empty string rather than a NULL."
  (if (string= string "") nil string))

(defun right-trim (string)
  "Remove whitespaces at end of STRING."
  (declare (type simple-string string))
  (string-right-trim '(#\Space) string))

(defun byte-vector-to-bytea (vector)
  "Transform a simple array of unsigned bytes to the PostgreSQL bytea
  representation as documented at
  http://www.postgresql.org/docs/9.3/interactive/datatype-binary.html

  Note that we choose here the bytea Hex Format."
  (declare (type (or null (simple-array (unsigned-byte 8) (*))) vector))
  (when vector
    (let ((hex-digits "0123456789abcdef")
	  (bytea (make-array (+ 2 (* 2 (length vector)))
			     :initial-element #\0
			     :element-type 'standard-char)))

      ;; The entire string is preceded by the sequence \x (to distinguish it
      ;; from the escape format).
      (setf (aref bytea 0) #\\)
      (setf (aref bytea 1) #\x)

      (loop for pos from 2 by 2
	 for byte across vector
	 do (let ((high (ldb (byte 4 4) byte))
		  (low  (ldb (byte 4 0) byte)))
	      (setf (aref bytea pos)       (aref hex-digits high))
	      (setf (aref bytea (+ pos 1)) (aref hex-digits low)))
	 finally (return bytea)))))

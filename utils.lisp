;;;
;;; Random utilities
;;;
(in-package :pgloader.utils)

;;;
;;; Timing Macro
;;;
(defun elapsed-time-since (start)
  "Return how many seconds ticked between START and now"
  (/ (- (get-internal-real-time) start)
     internal-time-units-per-second))

(defmacro timing (&body forms)
  "return both how much real time was spend in body and its result"
  (let ((start (gensym))
	(end (gensym))
	(result (gensym)))
    `(let* ((,start (get-internal-real-time))
	    (,result (progn ,@forms))
	    (,end (get-internal-real-time)))
       (values ,result (/ (- ,end ,start) internal-time-units-per-second)))))

;;;
;;; Timing Formating
;;;
(defun format-interval (seconds &optional (stream t))
  "Output the number of seconds in a human friendly way"
  (multiple-value-bind (years months days hours mins secs millisecs)
      (date:decode-interval (date:encode-interval :second seconds))
    (format
     stream
     "~:[~*~;~d years ~]~:[~*~;~d months ~]~:[~*~;~d days ~]~:[~*~;~dh~]~:[~*~;~dm~]~d.~ds"
     (< 0 years)  years
     (< 0 months) months
     (< 0 days)   days
     (< 0 hours)  hours
     (< 0 mins)   mins
     secs (truncate millisecs))))

;;;
;;; Data Structures to maintain information about loading state
;;;
(defstruct pgtable
  name
  (read 0   :type fixnum)		; how many rows did we read
  (rows 0   :type fixnum)		; how many rows did we write
  (errs 0   :type fixnum)		; how many errors did we see
  (secs 0.0 :type float)		; how many seconds did it take
  reject-data reject-logs)		; files where to find reject data

(defstruct pgstate
  (tables (make-hash-table :test 'equal))
  (read 0   :type fixnum)
  (rows 0   :type fixnum)
  (errs 0   :type fixnum)
  (secs 0.0 :type float))

(defun pgstate-get-table (pgstate name)
  (gethash name (pgstate-tables pgstate)))

(defun pgstate-add-table (pgstate dbname table-name)
  "Instanciate a new pgtable structure to hold our stats, and return it."
  (or (pgstate-get-table pgstate table-name)
      (let ((table (setf (gethash table-name (pgstate-tables pgstate))
			 (make-pgtable :name table-name))))
	(setf (pgtable-reject-data table)
	      (make-pathname
	       :directory (pathname-directory
			   (merge-pathnames
			    (format nil "~a" dbname) *reject-path-root*))
	       :name table-name
	       :type "dat")
	      (pgtable-reject-logs table)
	      (make-pathname
	       :directory (pathname-directory
			   (merge-pathnames
			    (format nil "~a" dbname) *reject-path-root*))
	       :name table-name
	       :type "log"))
	table)))

(defun pgstate-setf (pgstate name &key read rows errs secs)
  (let ((pgtable (pgstate-get-table pgstate name)))
    (when read
      (setf (pgtable-read pgtable) read)
      (incf (pgstate-read pgstate) read))
    (when rows
      (setf (pgtable-rows pgtable) rows)
      (incf (pgstate-rows pgstate) rows))
    (when errs
      (setf (pgtable-errs pgtable) errs)
      (incf (pgstate-errs pgstate) errs))
    (when secs
      (setf (pgtable-secs pgtable) secs)
      (incf (pgstate-secs pgstate) secs))
    pgtable))

(defun pgstate-incf (pgstate name &key rows errs secs)
  (let ((pgtable (pgstate-get-table pgstate name)))
    (when rows
      (incf (pgtable-rows pgtable) rows)
      (incf (pgstate-rows pgstate) rows))
    (when errs
      (incf (pgtable-errs pgtable) errs)
      (incf (pgstate-errs pgstate) errs))
    (when secs
      (incf (pgtable-secs pgtable) secs)
      (incf (pgstate-secs pgstate) secs))
    pgtable))

(defun pgstate-decf (pgstate name &key rows errs secs)
  (let ((pgtable (pgstate-get-table pgstate name)))
    (when rows
      (decf (pgtable-rows pgtable) rows)
      (decf (pgstate-rows pgstate) rows))
    (when errs
      (decf (pgtable-errs pgtable) errs)
      (decf (pgstate-errs pgstate) errs))
    (when secs
      (decf (pgtable-secs pgtable) secs)
      (decf (pgstate-secs pgstate) secs))
    pgtable))

(defun report-pgtable-stats (pgstate name)
  (with-slots (rows errs secs) (pgstate-get-table pgstate name)
    (format t "~9@a  ~9@a  ~9@a" rows errs (format-interval secs nil))))

(defun report-pgstate-stats (pgstate legend)
  (with-slots (rows errs secs) pgstate
    (format t "~&------------------------------  ---------  ---------  ---------")
    (format t "~&~30@a  ~9@a  ~9@a  ~9@a" legend
	    rows errs (format-interval secs nil))))

;;;
;;; Pretty print a report while doing bulk operations
;;;
(defun report-header ()
  (format t "~&~30@a  ~9@a  ~9@a  ~9@a" "table name" "rows" "errors" "time")
  (format t "~&------------------------------  ---------  ---------  ---------"))

(defun report-table-name (table-name)
  (format t "~&~30@a  " table-name))

(defun report-results (rows errors seconds)
  (format t "~9@a  ~9@a  ~9@a" rows errors (format-interval seconds nil)))

(defun report-footer (legend rows errors seconds)
  (format t "~&------------------------------  ---------  ---------  ---------")
  (format t "~&~30@a  ~9@a  ~9@a  ~9@a" legend
	  rows errors (format-interval seconds nil)))


;;;
;;; Random utilities
;;;

(defpackage #:pgloader.utils
  (:use #:cl)
  (:export #:report-header
	   #:report-table-name
	   #:report-results
	   #:report-footer
	   #:format-interval
	   #:timing))

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


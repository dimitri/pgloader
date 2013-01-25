;;;
;;; Some little timing tools
;;;

(in-package :galaxya-loader)

;;;
;;; Timing Macros
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

(defun format-interval (seconds &optional (stream t))
  "Output the number of seconds in a human friendly way"
  (multiple-value-bind (year month day hour minute second millisecond)
      (date:decode-interval (date:encode-interval :second seconds))
    (declare (ignore year month))
    (when (< 0 day)  (format stream "~d days " day))
    (when (< 0 hour) (format stream "~d hour " hour))
    (format stream "~dm~ds.~d" minute second millisecond)))
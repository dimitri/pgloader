;;;
;;; Random utilities
;;;
(in-package :pgloader.utils)

;;;
;;; Timing Macro
;;;
(defun elapsed-time-since (start)
  "Return how many seconds ticked between START and now"
  (let ((now (get-internal-real-time)))
    (coerce (/ (- now start) internal-time-units-per-second) 'double-float)))

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
    (declare (ignore millisecs))
    (format
     stream
     "~:[~*~;~d years ~]~:[~*~;~d months ~]~:[~*~;~d days ~]~:[~*~;~dh~]~:[~*~;~dm~]~5,3fs"
     (< 0 years)  years
     (< 0 months) months
     (< 0 days)   days
     (< 0 hours)  hours
     (< 0 mins)   mins
     (+ secs (- (multiple-value-bind (r q)
		    (truncate seconds 60)
		  (declare (ignore r))
		  q)
		secs)))))

;;;
;;; Camel Case converter
;;;
(defun camelCase-to-colname (string)
  "Transform input STRING into a suitable column name.
    lahmanID        lahman_id
    playerID        player_id
    birthYear       birth_year"
  (coerce
   (loop
      for first = t then nil
      for char across string
      for previous-upper-p = nil then char-upper-p
      for char-upper-p = (eq char (char-upcase char))
      for new-word = (and (not first) char-upper-p (not previous-upper-p))
      when (and new-word (not (char= char #\_))) collect #\_
      collect (char-downcase char))
   'string))

;;;
;;; Unquote SQLite default values, might be useful elsewhere
;;;
(defun unquote (string &optional (quote #\'))
  "Given '0', returns 0."
  (declare (type (or null simple-string) string))
  (when string
    (let ((l (length string)))
      (if (char= quote (aref string 0) (aref string (1- l)))
          (subseq string 1 (1- l))
          string))))

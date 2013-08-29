;;;
;;; Implement a very simple syslog UDP server in order to be able to stream
;;; syslog messages directly into a PostgreSQL database, using the COPY
;;; protocol.
;;;

(in-package #:pgloader.syslog)

(defvar *buffer* ""   "Buffer we are currently parsing")
(defvar *position* 0  "Reading position in the message")
(defvar *message* nil "Current message parsed from the *buffer*")

;;
;; Those "constants" straight from the RFC 3164
;;
(defvar *facility-bytespec* (byte 3 0) "Read facility from priority")
(defvar *severity-bytespec* (byte 5 3) "Read severity from priority")

(defvar *space-code-set*   #. (quote (coerce " " 'list)))
(defvar *decimal-code-set* #. (quote (coerce "0123456789" 'list)))
(defvar *header-code-set*  #. (quote (loop
					:for i :from 32 :to 126
					:collect (code-char i))))
(defvar *alphabetical-code-set*
  #. (quote (concatenate 'list
			 "abcdefghijklmnopqrstuvwxyz"
			 "ABCDEFGHIJKLMNOPQRSTUVWXYZ")))

(defvar *month-abbrevs*
  '("Jan" "Feb" "Mar" "Apr" "May" "Jun" "Jul" "Aug" "Sep" "Oct" "Nov" "Dec"))

(defun parse-priority (pri &aux (priority (parse-integer pri)))
  "Return facility and severity from PRI, as multiple values"
  (values (ldb *facility-bytespec* priority)
	  (ldb *severity-bytespec* priority)))

(defstruct message facility severity timestamp hostname tag content)

;;
;; Some low level utils
;;
(defun skip-one (character-bag)
  "Skip a character in the stream"
  ;; NOOP as of now TODO: signal a condition if current *buffer* *position*
  ;; is not containing a character of the bag
  (declare (ignore character-bag))
  (incf *position*))

(defun skip-many (character-bag)
  "Skip all characters from CHARACTER-BAG current *buffer*"
  (loop
     :while (member (aref *buffer* *position*) character-bag)
     :do (incf *position*)))

(defun collect-many (character-bag)
  "Return a string of the chars found in *buffer* at *position* that are in
   the given CHARACTER-BAG."
  (coerce
   (loop
      :until (= *position* (length *buffer*))
      :for char = (aref *buffer* *position*)
      :while (member char character-bag)
      :collect char
      :do (incf *position*))
   'string))

(defun collect-max (character-bag limit)
  "Return a string of the chars found in *buffer* at *position* that are in
   the given CHARACTER-BAG."
  (coerce
   (loop
      :repeat limit
      :for char = (aref *buffer* *position*)
      :while (member char character-bag)
      :collect char
      :do (incf *position*))
   'string))

(defun collect-until (character-bag)
  "Collect characters and increment *position* until we reach one of
   CHARACTER-BAG."
  (coerce
   (loop
      :for char = (aref *buffer* *position*)
      :until (member char character-bag)
      :collect char
      :do (incf *position*))
   'string))

(defun read-one-of (list-of-strings &optional length)
  "Return one of the strings given in LIST-OF-STRINGS and increment *position*."
  (loop
     :for match :in list-of-strings
     :for len = (or length (length match))
     :until (string= match
		     (coerce
		      (subseq *buffer* *position* (+ *position* len)) 'string))
     :finally (progn (incf *position* len)
		     (return match))))

;;
;; Read parts of the syslog message
;;
(defun read-priority ()
  "Reads the priority from the BUFFER position POS and return the priority
   and the next item position as multiple values"
  (skip-one "<")
  (multiple-value-bind (facility severity)
      (parse-priority (collect-many *decimal-code-set*))
    (setf (message-facility *message*) facility
	  (message-severity *message*) severity))
  (skip-one ">"))

(defun read-spaces-and-number ()
  (skip-many *space-code-set*)
  (let ((digits (collect-many *decimal-code-set*)))
    (or (parse-integer digits :junk-allowed t) 0)))

(defun maybe-read-year ()
  "See if there's a year and/or a TZ at current *position* in *buffer*."
  (let* ((start-position  *position*)
	 (ignore-space    (skip-one " "))
	 (digits-at-point (collect-many *decimal-code-set*)))
    (declare (ignore ignore-space))
    (if (and digits-at-point
	     (char= (aref *buffer* *position*) #\Space)
	     (or (= 2 (length digits-at-point))
		 (= 4 (length digits-at-point))))
	(parse-integer digits-at-point)
	;; that was not a year after all
	(prog2 (setf *position* start-position)
	    ;; get current year
	    (nth 5 (multiple-value-list (get-decoded-time)))))))

(defun read-timestamp ()
  "The TIMESTAMP field is the local time and is in the format of \"Mmm dd
   hh:mm:ss\" (without the quote marks)"
  (multiple-value-bind (month day hours mins secs year)
      (values (+ 1 (position (read-one-of *month-abbrevs*)
			     *month-abbrevs*
			     :test #'string=)) ; month
	      (read-spaces-and-number)	       ; day
	      (read-spaces-and-number)	       ; hours
	      (prog2 (skip-one ":")	       ;
		  (read-spaces-and-number))    ; mins
	      (prog2 (skip-one ":")	       ;
		  (read-spaces-and-number))    ; secs
	      (maybe-read-year))	       ; year
    (setf (message-timestamp *message*)
	  (local-time:encode-timestamp 0 secs mins hours day month year)))))

(defun read-hostname ()
  "Any non whitespace is constituent of the HOSTNAME field."
  (setf (message-hostname *message*) (collect-until *space-code-set*)))

(defun read-header ()
  "Read the HEADER part of a syslog UDP buffer"
  (read-timestamp)
  (skip-one " ")
  (read-hostname)
  (skip-one " "))

(defun read-tag ()
  (setf (message-tag *message*) (collect-max *alphabetical-code-set* 32)))

(defun read-content ()
  (setf (message-content *message*) (subseq *buffer* *position*)))

(defun read-msg ()
  (read-tag)
  (read-content))

(defun parse-message (&key
			((:buffer *buffer*) *buffer*)
			((:position *position*) *position*))
  "Parse a whole syslog message and return a message structure."
  ;; (declare (type (simple-array (unsigned-byte 8) *) *buffer*))
  (let ((*message* (make-message)))
    (read-priority)
    (read-header)
    (read-msg)
    *message*))

(defun syslog-udp-handler (buffer)
  "Handler to register to usocket:socket-server call."
  (declare (type (simple-array (unsigned-byte 8) *) buffer))
  (let* ((*buffer* buffer)
	 (*position* 0)
	 (mesg (parse-message)))))


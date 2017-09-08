;;;
;;; Logs
;;;
(in-package :pgloader.logs)

(defcategory :panic)
(defcategory :fatal   (or :fatal :panic))
(defcategory :log     (or :log :fatal))
(defcategory :error   (or :error :log))
(defcategory :warning (or :warning :error))
(defcategory :notice  (or :notice :warning))
(defcategory :sql     (or :sql :notice))
(defcategory :info    (or :info :sql))
(defcategory :debug   (or :debug :info))
(defcategory :data    (or :data :debug))

(defvar *log-messengers* nil
  "Currently active log-messengers")

;; Start a messenger to store our message into
(defun start-logger (&key
		       (log-filename *log-filename*)
		       ((:log-min-messages *log-min-messages*) *log-min-messages*)
		       ((:client-min-messages *client-min-messages*) *client-min-messages*))
  "Start the pgloader log manager and messenger."

  (setf (log-manager)
	(make-instance 'log-manager
		       :message-class 'formatted-message))

  ;; we need an existing place where to log our messages
  (ensure-directories-exist log-filename)

  (push (cl-log:start-messenger 'text-file-messenger
				:name "logfile"
				:filter *log-min-messages*
				:filename log-filename
                                :external-format :utf-8)
	*log-messengers*)

  (push (cl-log:start-messenger 'text-stream-messenger
				:name "stdout"
				:filter *client-min-messages*
				:stream (make-broadcast-stream *standard-output*))
	*log-messengers*)

  (cl-log:log-message :notice "Starting pgloader, log system is ready."))

(defun stop-logger ()
  "Stop the pgloader manager and messengers."

  (loop for messenger = (pop *log-messengers*)
     while messenger
     do (cl-log:stop-messenger messenger)))

;; monkey patch the print-object method for cl-log timestamp
(defconstant +nsec+ (* 1000 1000 1000)
  "How many nanoseconds do you find in a second")

(defun fraction-to-nsecs (fraction)
  "FRACTION is a number of internal-time-units-per-second, return nsecs"
  (declare (inline fraction-to-nsecs) (fixnum fraction))
  (floor (/ (* fraction +nsec+) internal-time-units-per-second)))

(defmethod print-object ((self cl-log:timestamp) stream)
  "we want to print human readable timestamps"
  (let ((log-local-time
	 (local-time:universal-to-timestamp
	  (cl-log:timestamp-universal-time self)
	  :nsec (fraction-to-nsecs (cl-log:timestamp-fraction self)))))
    (local-time:format-timestring stream log-local-time)))


;;;
;;; Implement a very simple syslog UDP server in order to be able to stream
;;; syslog messages directly into a PostgreSQL database, using the COPY
;;; protocol.
;;;

(in-package #:pgloader.syslog)

(defun process-message (data queue)
  "Process the data: send it in the queue for PostgreSQL COPY handling."
  (lq:push-queue data queue))

(defun syslog-udp-handler (buffer scanners)
  "Handler to register to usocket:socket-server call."
  (declare (type (simple-array (unsigned-byte 8) *) buffer))
  (let* ((mesg (map 'string #'code-char buffer)))
    (loop
       for (&key parser queue &allow-other-keys) in scanners
       for (match parts) = (multiple-value-list
			    (cl-ppcre:scan-to-strings parser mesg))
       until match
       finally (when match
		 (process-message (coerce parts 'list) queue)))))

(defun start-syslog-server (&key
			      scanners
			      (host nil)
			      (port 10514))
  "Start our syslog socket and read messages."
  (usocket:socket-server host port #'syslog-udp-handler (list scanners)
			 :protocol :datagram
			 :reuse-address t
			 :element-type '(unsigned-byte 8)))

(defun stream-messages (&key scanners host port truncate transforms)
  "Listen for syslog messages on given UDP PORT, and stream them to PostgreSQL"
  (let* ((nb-tasks    (+ 1 (length scanners)))
	 (lp:*kernel* (lparallel:make-kernel nb-tasks))
	 (channel     (lp:make-channel))
	 ;; add a data queue to each scanner
	 (scanners    (loop
			 for scanner in scanners
			 for dataq = (lq:make-queue :fixed-capacity 4096)
			 collect (append scanner (list :queue dataq)))))
    ;; listen to syslog messages and match them against the scanners
    (lp:submit-task channel
		    #'start-syslog-server :scanners scanners :host host :port port)

    ;; and start another task per scanner to pull that data from the queue
    ;; to PostgreSQL, each will have a specific connection and queue.
    (loop
       for (&key target gucs queue &allow-other-keys) in scanners
       do (destructuring-bind (&key host port user password
				    dbname table-name
				    &allow-other-keys)
	      target
	    (let* ((*pgconn-host* host)
		   (*pgconn-port* port)
		   (*pgconn-user* user)
		   (*pgconn-pass* password)
		   (*pg-settings* gucs))
	      (declare (special *pgconn-host* *pgconn-port*
				*pgconn-user* *pgconn-pass*
				*pg-settings*))
	      (lp:submit-task channel
			      (lambda ()
				(pgloader.pgsql:copy-from-queue
				 dbname
				 table-name
				 queue
				 :truncate truncate
				 :transforms transforms))))))

    ;; now wait until both the tasks are over
    (loop for tasks below nb-tasks do (lp:receive-result channel))))

;;;
;;; A test client, with some not-so random messages
;;;
(defun send-message (message
		     &key
		       (host "localhost")
		       (port 10514))
  (let ((socket (usocket:socket-connect host port :protocol :datagram)))
    (usocket:socket-send socket message (length message))))

#|

(send-message "<165>1 2013-08-29T00:30:12Z Dimitris-MacBook-Air.local coreaudiod[215]: - - Enabled automatic stack shots because audio IO is inactive")

(send-message "<0>Aug 31 21:38:28 Dimitris-MacBook-Air.local Google Chrome[236]: notify name \"com.apple.coregraphics.GUIConsoleSessionChanged\" has been registered 20 times - this may be a leak")


SYSLOG> (start-syslog-server
	 :scanners (list (abnf:parse-abnf-grammar abnf:*abnf-rsyslog*
						  :rsyslog-msg
						  :registering-rules '(:timestamp :app-name :data))))
data: 
STYLE-WARNING: redefining PGLOADER.SYSLOG::SYSLOG-UDP-HANDLER in DEFUN

|#

;;;
;;; Implement a very simple syslog UDP server in order to be able to stream
;;; syslog messages directly into a PostgreSQL database, using the COPY
;;; protocol.
;;;

(in-package #:pgloader.syslog)

(defun process-message (data)
  "Process the data: send it in the queue for PostgreSQL COPY handling."
  (format t "data: ~{~a~^, ~}~%" data))

(defun syslog-udp-handler (buffer scanners)
  "Handler to register to usocket:socket-server call."
  (declare (type (simple-array (unsigned-byte 8) *) buffer))
  (let* ((mesg (map 'string #'code-char buffer)))
    (loop
       for scanner in scanners
       for (match parts) = (multiple-value-list
			    (cl-ppcre:scan-to-strings scanner mesg))
       until match
       finally (process-message (coerce parts 'list)))))

(defun start-syslog-server (&key
			      scanners
			      (host nil)
			      (port 10514))
  "Start our syslog socket and read messages."
  (usocket:socket-server host port #'syslog-udp-handler (list scanners)
			 :protocol :datagram
			 :reuse-address t
			 :element-type '(unsigned-byte 8)))

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

|#

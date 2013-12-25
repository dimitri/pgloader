;;;
;;; Central thread that deals with monitoring
;;;
;;; Manages the logging from a single thread while another bunch of threads
;;; are doing the data processing and loading, and maintain states.
;;;
;;; The public API is the macro with-monitor and the function log-message,
;;; that shares its signature with cl-log:log-message so as to be a drop-in
;;; replacement. The only expected difference is for
;;; pgloader.monitor:log-message to send the message to a single central
;;; thread where the logging happen.
;;;
(in-package :pgloader.monitor)

(defvar *monitoring-queue* nil
  "Internal lparallel queue where to send and receive messages from.")

(defvar *monitoring-channel* nil
  "Internal lparallel channel.")

(defun send-event (event &rest params)
  "Add a new event to be processed by the monitor."
  (assert (not (null *monitoring-queue*)))
  (lq:push-queue (list event params) *monitoring-queue*))

(defun log-message (category description &rest arguments)
  "Send given message into our monitoring queue for processing."
  (apply #'send-event :log category description arguments))

(defun start-monitor (&key
                        (start-logger t)

                        ((:queue *monitoring-queue*) *monitoring-queue*)

                        ((:log-filename *log-filename*) *log-filename*)

                        ((:log-min-messages *log-min-messages*)
                         *log-min-messages*)

                        ((:client-min-messages *client-min-messages*)
                         *client-min-messages*))
  "Start the monitor and its logger."
  (let* ((bindings  `((*log-filename*        . ,*log-filename*)
                      (*log-min-messages*    . ,*log-min-messages*)
                      (*client-min-messages* . ,*client-min-messages*)
                      (*monitoring-queue*    . ,*monitoring-queue*)
                      (*error-output*        . ,*error-output*)
                      (*standard-output*     . ,*standard-output*)))
         (lparallel:*kernel*   (lp:make-kernel 1 :bindings bindings))
         (*monitoring-channel* (lp:make-channel)))

    (lp:submit-task *monitoring-channel* #'monitor *monitoring-queue*)
    (send-event :start :start-logger start-logger)

    *monitoring-channel*))

(defun stop-monitor (&key
                       (channel *monitoring-channel*)
                       (stop-logger t))
  "Stop the current monitor task."
  (send-event :stop :stop-logger stop-logger)
  (lp:receive-result channel))

(defmacro with-monitor ((&key (start-logger t)) &body body)
  "Start and stop the monitor around BODY code. The monitor is responsible
  for processing logs into a central logfile"
  `(let* ((*monitoring-queue*   (lq:make-queue))
          (*monitoring-channel* (start-monitor :start-logger ,start-logger)))
     (unwind-protect
          ,@body
       (stop-monitor :channel *monitoring-channel*
                     :stop-logger ,start-logger))))

(defun monitor (queue)
  "Receives and process messages from *monitoring-queue*."

  ;; process messages from the queue
  (loop for (event params) = (multiple-value-bind (element available)
                                 (lq:try-pop-queue queue)
                               (if available element (list :empty)))
     do
       (case event
         (:start  (progn
                    (destructuring-bind (&key start-logger) params
                      (when start-logger (pgloader.logs:start-logger)))
                    (cl-log:log-message :info "Starting monitor")))

         (:stop   (progn
                    (cl-log:log-message :info "Stopping monitor")
                    (destructuring-bind (&key stop-logger) params
                      (when stop-logger (pgloader.logs:stop-logger)))))

         (:empty  (sleep 0.2))         ; avoid buzy looping

         (:log    (destructuring-bind (category description &rest arguments)
                      params
                    ;; cl-log:log-message is a macro, we can't use apply
                    ;; here, so we need to break a level of abstraction
                    (let ((mesg (if arguments
                                    (format nil "~{~}" description arguments)
                                    description)))
                     (cl-log:log-message category mesg)))))

     until (eq event :stop)))

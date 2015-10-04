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

(defvar *sections* nil
  "List of currently monitored activities (per category or concurrency.")


;;;
;;; The external monitor API, with messages
;;;
(defstruct start start-logger)
(defstruct stop  stop-logger)
(defstruct noop)
(defstruct log-message category description arguments)
(defstruct new-label dbname section label)
(defstruct update-stats section label read rows errs secs rs ws)

(defun log-message (category description &rest arguments)
  "Send given message into our monitoring queue for processing."
  (send-event (make-log-message :category category
                                :description description
                                :arguments arguments)))


;;;
;;; Now, the monitor thread management
;;;
(defun send-event (event)
  "Add a new event to be processed by the monitor."
  (assert (not (null *monitoring-queue*)))
  (lq:push-queue event *monitoring-queue*))

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
    (send-event (make-start :start-logger start-logger))

    *monitoring-channel*))

(defun stop-monitor (&key
                       (channel *monitoring-channel*)
                       (stop-logger t))
  "Stop the current monitor task."
  (send-event (make-stop :stop-logger stop-logger))
  (lp:receive-result channel))

(defmacro with-monitor ((&key (start-logger t)) &body body)
  "Start and stop the monitor around BODY code. The monitor is responsible
  for processing logs into a central logfile"
  `(if ,start-logger
       (let* ((*monitoring-queue*   (lq:make-queue))
              (*monitoring-channel* (start-monitor :start-logger ,start-logger)))
         (unwind-protect
              ,@body
           (stop-monitor :channel *monitoring-channel*
                         :stop-logger ,start-logger)))

       ;; logger has already been started
       (progn ,@body)))

(defun monitor (queue)
  "Receives and process messages from *monitoring-queue*."

  ;; process messages from the queue
  (loop :for event := (multiple-value-bind (event available)
                          (lq:try-pop-queue queue)
                        (if available event (make-noop)))
     :do (typecase event
           (start
            (when (start-start-logger event)
              (pgloader.logs:start-logger))
            (cl-log:log-message :info "Starting monitor"))

           (stop
            (cl-log:log-message :info "Stopping monitor")
            (when (stop-stop-logger event) (pgloader.logs:stop-logger)))

           (noop
            (sleep 0.2))                ; avoid buzy looping

           (log-message
            ;; cl-log:log-message is a macro, we can't use apply
            ;; here, so we need to break a level of abstraction
            (let ((mesg (if (log-message-arguments event)
                            (format nil "~{~}"
                                    (log-message-description event)
                                    (log-message-arguments event))
                            (log-message-description event))))
              (cl-log:log-message (log-message-category event) "~a" mesg))))

     :until (typep event 'stop)))

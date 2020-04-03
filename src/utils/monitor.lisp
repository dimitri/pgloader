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

(defvar *monitoring-kernel* nil
  "Internal lparallel kernel to manage the separate monitor thread.")

(defvar *monitoring-queue* nil
  "Internal lparallel queue where to send and receive messages from.")

(defvar *monitoring-channel* nil
  "Internal lparallel channel.")

(defvar *sections* (create-state)
  "Global pgloader state, maintained by the dedicated monitor thread.")


;;;
;;; The external monitor API, with messages
;;;
(defstruct start start-logger)
(defstruct stop  stop-logger)
(defstruct report-summary reset)
(defstruct log-message category description arguments)
(defstruct new-label section label dbname)
(defstruct update-stats section label read rows errs secs rs ws bytes start stop)
(defstruct bad-row section label condition data)

(define-condition monitor-error (error)
  ((root-cause :initarg :root-cause :reader monitor-real-error))
  (:report (lambda (err stream)
             (format stream "FATAL: Failed to start the monitor thread.~%")
             (format stream "~%~a~%" (monitor-real-error err)))))

(defmacro log-message (category description &rest arguments)
  "Protect against evaluating ARGUMENTS in cases where we don't log at the
   given CATEGORY log-level."
  `(when (cl-log::category-messengers ,category)
     (send-log-message ,category ,description ,@arguments)))

(defun send-log-message (category description &rest arguments)
  "Send given message into our monitoring queue for processing."
  (when (cl-log::category-messengers category)
    (send-event (make-log-message :category category
                                  :description description
                                  :arguments arguments))))

(defun new-label (section label &optional dbname)
  "Send an event to create a new LABEL for registering a shared state under
   SECTION."
  (send-event (make-new-label :section section :label label :dbname dbname)))

(defun update-stats (section label
                     &key read rows errs secs rs ws bytes start stop)
  "Send an event to update stats for given SECTION and LABEL."
  (send-event (make-update-stats :section section
                                 :label label
                                 :read read
                                 :rows rows
                                 :errs errs
                                 :secs secs
                                 :rs rs
                                 :ws ws
                                 :bytes bytes
                                 :start start
                                 :stop stop)))

(defun process-bad-row (table condition data)
  "Send an event to log the bad row DATA in the reject and log files for given
   TABLE-NAME (a label in section :data), for reason found in CONDITION."
  (send-event (make-bad-row :section :data
                            :label table
                            :condition condition
                            :data data)))

(defun flush-summary (&key reset)
  (send-event (make-report-summary :reset reset)))

;;;
;;; Easier API to manage statistics collection and state updates
;;;
(defmacro with-stats-collection ((table-name
                                  &key
                                  (section :data)
                                  dbname
                                  use-result-as-read
                                  use-result-as-rows)
				 &body forms)
  "Measure time spent in running BODY into STATE, accounting the seconds to
   given DBNAME and TABLE-NAME"
  (let ((result (gensym "result"))
        (secs   (gensym "secs")))
    `(prog2
         (new-label ,section ,table-name ,dbname)
         (multiple-value-bind (,result ,secs)
             (timing ,@forms)
           (cond ((and ,use-result-as-read ,use-result-as-rows)
                  (update-stats ,section ,table-name
                                :read ,result :rows ,result :secs ,secs))
                 (,use-result-as-read
                  (update-stats ,section ,table-name :read ,result :secs ,secs))
                 (,use-result-as-rows
                  (update-stats ,section ,table-name :rows ,result :secs ,secs))
                 (t
                  (update-stats ,section ,table-name :secs ,secs)))
           ,result))))


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
                      (*root-dir*            . ,*root-dir*)
                      (*standard-output*     . ,*standard-output*)
                      (*summary-pathname*    . ,*summary-pathname*)
                      (*sections*            . ',*sections*)))
         (kernel      (lp:make-kernel 1 :bindings bindings))
         (lparallel:*kernel* kernel)
         (lparallel:*task-category* :monitor))

    ;; make our kernel and channel visible from the outside
    (setf *monitoring-kernel* kernel
          *monitoring-channel* (lp:make-channel)
          *monitoring-queue*   (lq:make-queue))

    (lp:task-handler-bind
        (#+pgloader-image
         (error
          #'(lambda (c)
              ;; we can't log-message a monitor thread error
              (lp:invoke-transfer-error
               (make-instance 'monitor-error :root-cause c)))))

      ;; warm up the channel to ensure we don't loose any event
      (lp:submit-task *monitoring-channel* '+ 1 2 3)
      (lp:receive-result *monitoring-channel*)

      ;; now that we know the channel is ready, start our long-running monitor
      (lp:submit-task *monitoring-channel* #'monitor *monitoring-queue*)
      (send-event (make-start :start-logger start-logger)))

    (values *monitoring-kernel* *monitoring-queue* *monitoring-channel*)))

(defun stop-monitor (&key
                       (kernel  *monitoring-kernel*)
                       (channel *monitoring-channel*)
                       (stop-logger t))
  "Stop the current monitor task."
  (send-event (make-stop :stop-logger stop-logger))
  (lp:receive-result channel)

  (let ((lp:*kernel* kernel))
    (lp:end-kernel :wait t)))

(defun call-with-monitor (thunk)
  "Call THUNK in a context where a monitor thread is active."
  (multiple-value-bind (*monitoring-kernel*
                        *monitoring-queue*
                        *monitoring-channel*)
      (start-monitor)
    (unwind-protect
         (funcall thunk)
      (stop-monitor))))

(defmacro with-monitor ((&key (start-logger t)) &body body)
  "Start and stop the monitor around BODY code. The monitor is responsible
  for processing logs into a central logfile"
  `(if ,start-logger
       (let ((*sections* (create-state)))
         (call-with-monitor #'(lambda () ,@body)))
       (let ((*sections* (create-state)))
         ,@body)))

(defun monitor (queue)
  "Receives and process messages from *monitoring-queue*."

  ;; process messages from the queue
  (loop :with start-time := (get-internal-real-time)

     :for event := (lq:pop-queue queue)
     :do (typecase event
           (start
            (when (start-start-logger event)
              (pgloader.logs:start-logger))
            (cl-log:log-message :info "Starting monitor")
            (cl-log:log-message :log "pgloader version ~s" *version-string*))

           (stop
            (cl-log:log-message :info "Stopping monitor")

            ;; time to shut down the logger?
            (when (stop-stop-logger event)
              (pgloader.logs:stop-logger)))

           (report-summary
            (cl-log:log-message :log "report summary ~@[reset~]"
                                (report-summary-reset event))
            (report-current-summary start-time)

            (when (report-summary-reset event)
              (setf *sections* (create-state))))

           (log-message
            ;; cl-log:log-message is a macro, we can't use apply
            ;; here, so we need to break a level of abstraction
            (let* ((*print-circle* t)
                   (mesg (if (log-message-arguments event)
                             (format nil "~{~}"
                                     (log-message-description event)
                                     (log-message-arguments event))
                             (log-message-description event))))
              (cl-log:log-message (log-message-category event) "~a" mesg)))

           (new-label
            (let* ((section
                    (get-state-section *sections*
                                       (new-label-section event)))
                   (label
                    (pgstate-new-label section
                                       (new-label-label event))))

              (when (eq :data (new-label-section event))
                (pgtable-initialize-reject-files label
                                                 (new-label-dbname event)))))

           (update-stats
            (let* ((pgstate (get-state-section *sections*
                                               (update-stats-section event)))
                   (label   (update-stats-label event))
                   (table   (pgstate-new-label pgstate label)))

              (pgstate-incf pgstate label
                            :read (update-stats-read event)
                            :rows (update-stats-rows event)
                            :errs (update-stats-errs event)
                            :secs (update-stats-secs event)
                            :rs   (update-stats-rs event)
                            :ws   (update-stats-ws event)
                            :bytes (update-stats-bytes event))

              (maybe-log-progress-message event label table)

              (when (update-stats-start event)
                (process-update-stats-start-event event label table))

              (when (update-stats-stop event)
                (process-update-stats-stop-event event label table))))

           (bad-row
            (let* ((pgstate (get-state-section *sections* :data))
                   (label   (bad-row-label event))
                   (table   (pgstate-get-label pgstate label)))
              (pgstate-incf pgstate label :errs 1)
              (%process-bad-row table
                                (bad-row-condition event)
                                (bad-row-data event)))))

     :until (typep event 'stop)))

(defun process-update-stats-start-event (event label table)
  (declare (type update-stats event))
  (cl-log:log-message :debug "start ~a ~30t ~a"
                      (pgloader.catalog:format-table-name label)
                      (update-stats-start event))
  (setf (pgtable-start table) (update-stats-start event)))

(defun process-update-stats-stop-event (event label table)
  (declare (type update-stats event))
  ;; each PostgreSQL writer thread will send a stop even, here
  ;; we only keep the latest one.
  (when (or (null (pgtable-stop table))
            (< (pgtable-stop table) (update-stats-stop event)))
    (setf (pgtable-stop table) (update-stats-stop event))
    (let ((secs (elapsed-time-since (pgtable-start table)
                                    (pgtable-stop table))))
      (setf (pgtable-secs table) secs)

      (cl-log:log-message :debug " stop ~a ~30t | ~a .. ~a = ~a"
                          (pgloader.catalog:format-table-name label)
                          (pgtable-start table)
                          (pgtable-stop table)
                          secs))))

(defun maybe-log-progress-message (event label table)
  "Log some kind of a “keep alive” message to the user, for the sake of
   showing progress.

   Something like one message every 10 batches should only target big tables
  where we have to wait for a pretty long time."
  (when (and (update-stats-rows event)
             (typep label 'pgloader.catalog:table)
             (< (* 9 *copy-batch-rows*)
                (mod (pgtable-rows table)
                     (* 10 *copy-batch-rows*))))
    (cl-log:log-message :notice "copy ~a: ~d rows done, ~7<~a~>, ~9<~a~>"
                        (pgloader.catalog:format-table-name label)
                        (pgtable-rows table)
                        (pgloader.utils:pretty-print-bytes
                         (pgtable-bytes table))
                        (pgloader.utils:pretty-print-bytes
                         (truncate (pgtable-bytes table)
                                   (elapsed-time-since
                                    (pgtable-start table)))
                         :unit "Bps"))))

(defun report-current-summary (start-time)
  "Print out the current summary."
  (let* ((summary-stream (when *summary-pathname*
                           (open *summary-pathname*
                                 :direction :output
                                 :external-format :utf-8
                                 :if-exists :rename
                                 :if-does-not-exist :create)))
         (*report-stream* (or summary-stream *standard-output*)))
    (report-full-summary *sections*
                         "Total import time"
                         (elapsed-time-since start-time))
    (when summary-stream (close summary-stream))))


;;;
;;; Internal utils
;;;
(defun elapsed-time-since (start &optional (end (get-internal-real-time)))
  "Return how many seconds ticked between START and now"
  (let ((end (or end (get-internal-real-time))))
    (coerce (/ (- end start) internal-time-units-per-second) 'double-float)))


;;;
;;; Timing Macro
;;;
(defmacro timing (&body forms)
  "return both how much real time was spend in body and its result"
  (let ((start (gensym))
	(end (gensym))
	(result (gensym)))
    `(let* ((,start (get-internal-real-time))
	    (,result (progn ,@forms))
	    (,end (get-internal-real-time)))
       (values ,result (/ (- ,end ,start) internal-time-units-per-second)))))

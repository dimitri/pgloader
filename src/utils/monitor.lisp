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

(defvar *sections* '(:pre nil :data nil :post nil)
  "plist of load sections: :pre, :data and :post.")


;;;
;;; The external monitor API, with messages
;;;
(defstruct start start-logger)
(defstruct stop  stop-logger)
(defstruct report-summary reset)
(defstruct noop)
(defstruct log-message category description arguments)
(defstruct new-label section label dbname)
(defstruct update-stats section label read rows errs secs rs ws)
(defstruct bad-row section label condition data)

(defun log-message (category description &rest arguments)
  "Send given message into our monitoring queue for processing."
  (send-event (make-log-message :category category
                                :description description
                                :arguments arguments)))

(defun new-label (section label &optional dbname)
  "Send an event to create a new LABEL for registering a shared state under
   SECTION."
  (send-event (make-new-label :section section :label label :dbname dbname)))

(defun update-stats (section label &key read rows errs secs rs ws)
  "Send an event to update stats for given SECTION and LABEL."
  (send-event (make-update-stats :section section
                                 :label label
                                 :read read
                                 :rows rows
                                 :errs errs
                                 :secs secs
                                 :rs rs
                                 :ws ws)))

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
                      (*standard-output*     . ,*standard-output*)
                      (*summary-pathname*    . ,*summary-pathname*)
                      (*sections*            . ',*sections*)))
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
  `(let ((*sections* (list :pre  (make-pgstate)
                           :data (make-pgstate)
                           :post (make-pgstate))))
     (if ,start-logger
         (let* ((*monitoring-queue*   (lq:make-queue))
                (*monitoring-channel* (start-monitor :start-logger ,start-logger)))
           (unwind-protect
                ,@body
             (stop-monitor :channel *monitoring-channel*
                           :stop-logger ,start-logger)))

         ;; logger has already been started
         (progn ,@body))))

(defun monitor (queue)
  "Receives and process messages from *monitoring-queue*."

  ;; process messages from the queue
  (loop :with start-time := (get-internal-real-time)

     :for event := (multiple-value-bind (event available)
                       (lq:try-pop-queue queue)
                     (if available event (make-noop)))
     :do (typecase event
           (start
            (when (start-start-logger event)
              (pgloader.logs:start-logger))
            (cl-log:log-message :info "Starting monitor"))

           (stop
            (cl-log:log-message :info "Stopping monitor")

            ;; report the summary now
            (destructuring-bind (&key pre data post) *sections*
              (unless (and (null pre) (null data) (null post))
                (report-current-summary start-time)))

            ;; time to shut down the logger?
            (when (stop-stop-logger event)
              (pgloader.logs:stop-logger)))

           (report-summary
            (report-current-summary start-time)

            (when (report-summary-reset event)
              (setf *sections* (list :pre  (make-pgstate)
                                     :data (make-pgstate)
                                     :post (make-pgstate)))))

           (noop
            (sleep 0.2))                ; avoid buzy looping

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
            (let ((label
                   (pgstate-new-label (getf *sections* (new-label-section event))
                                      (new-label-label event))))

              (when (eq :data (new-label-section event))
                (pgtable-initialize-reject-files label
                                                 (new-label-dbname event)))))

           (update-stats
            ;; it only costs an extra hash table lookup...
            (pgstate-new-label (getf *sections* (update-stats-section event))
                               (update-stats-label event))

            (pgstate-incf (getf *sections* (update-stats-section event))
                          (update-stats-label event)
                          :read (update-stats-read event)
                          :rows (update-stats-rows event)
                          :secs (update-stats-secs event)
                          :errs (update-stats-errs event)
                          :rs   (update-stats-rs event)
                          :ws   (update-stats-ws event)))

           (bad-row
            (%process-bad-row (bad-row-label event)
                              (bad-row-condition event)
                              (bad-row-data event))))

     :until (typep event 'stop)))

(defun report-current-summary (start-time)
  "Print out the current summary."
  (let* ((summary-stream (when *summary-pathname*
                           (open *summary-pathname*
                                 :direction :output
                                 :if-exists :rename
                                 :if-does-not-exist :create)))
         (*report-stream* (or summary-stream *standard-output*)))
    (report-full-summary "Total import time"
                         *sections*
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

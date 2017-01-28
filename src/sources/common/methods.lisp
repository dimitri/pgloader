;;;
;;; Generic API for pgloader sources
;;;
(in-package :pgloader.sources)

;;;
;;; Common API implementation
;;;
(defmethod queue-raw-data ((copy copy) queue-list)
  "Stream data as read by the map-queue method on the COPY argument into QUEUE,
   as given."
  (log-message :debug "Reader started for ~a" (format-table-name (target copy)))
  (let* ((start-time (get-internal-real-time))
         (blist  (loop :for queue :in queue-list :collect (make-batch)))
         (bclist (nconc blist blist))   ; build a circular list
         (qlist  (copy-list queue-list))
         (qclist (nconc qlist qlist))   ; build a circular list
         (process-row
          (lambda (row)
            (when (or (eq :data *log-min-messages*)
                      (eq :data *client-min-messages*))
              (log-message :data "< ~s" row))
            (prog1
                (setf (car bclist)      ; batch-row might create a new batch
                      (batch-row (car bclist) row (target copy) (car qclist)))
              ;; round robin on batches and queues
              (setf bclist (cdr bclist)
                    qclist (cdr qclist))))))

    ;; call the source-specific method for reading input data
    (map-rows copy :process-row-fn process-row)

    ;; process last batches and send them to queues
    ;; and mark end of stream
    (loop :repeat (length queue-list)
       :for batch :in blist
       :for queue :in qlist
       :do (progn
             (finish-batch batch (target copy) queue)
             (push-end-of-data-message queue)))

    (let ((seconds (elapsed-time-since start-time)))
      (log-message :debug "Reader for ~a is done in ~6$s"
                   (format-table-name (target copy)) seconds)
      (list :reader (target copy) seconds))))

(defmethod format-data-to-copy ((copy copy) input-queue output-queue
                                &optional pre-formatted)
  "Loop over the data in the RAW-QUEUE and prepare it in batches in the
   FORMATED-QUEUE, ready to be sent down to PostgreSQL using the COPY protocol."
  (log-message :debug "Transformer ~a in action for ~a!"
               (lp:kernel-worker-index)
               (format-table-name (target copy)))
  (let* ((start-time (get-internal-real-time))
         (preprocess (preprocess-row copy)))

    (loop :for (mesg batch count oversized?) := (lq:pop-queue input-queue)
       :until (eq :end-of-data mesg)
       :do (let ((batch-start-time (get-internal-real-time)))
             ;; transform each row of the batch into a copy-string
             (loop :for i :below count
                :do (let* ((row (if preprocess
                                    (funcall preprocess (aref batch i))
                                    (aref batch i)))
                           (copy-data
                            (handler-case
                                (format-vector-row row
                                                   (transforms copy)
                                                   pre-formatted)
                              (condition (e)
                                (log-message :error "~a" e)
                                (update-stats :data (target copy) :errs 1)
                                nil))))
                      (when (or (eq :data *log-min-messages*)
                                (eq :data *client-min-messages*))
                        (log-message :data "> ~s" copy-data))

                      (setf (aref batch i) copy-data)))

             ;; the batch is ready, log about it
             (log-message :debug "format-data-to-copy[~a] ~d row in ~6$s"
                          (lp:kernel-worker-index)
                          count
                          (elapsed-time-since batch-start-time))

             ;; and send the formatted batch of copy-strings down to PostgreSQL
             (lq:push-queue (list mesg batch count oversized?) output-queue)))

    ;; mark end of stream
    (push-end-of-data-message output-queue)

    ;; and return
    (let ((seconds (elapsed-time-since start-time)))
      (log-message :info "Transformer[~a] for ~a is done in ~6$s"
                   (lp:kernel-worker-index)
                   (format-table-name (target copy)) seconds)
      (list :worker (target copy) seconds))))

(defmethod preprocess-row ((copy copy))
  "The default preprocessing of raw data is to do nothing."
  nil)

(defmethod copy-column-list ((copy copy))
  "Default column list is an empty list."
  nil)

(defmethod copy-to ((copy copy) pgsql-copy-filename)
  "Extract data from COPY file into a PotgreSQL COPY TEXT formated file"
  (with-open-file (text-file pgsql-copy-filename
                             :direction :output
                             :if-exists :supersede
                             :external-format :utf-8)
    (let ((row-fn (lambda (row)
                    (format-vector-row text-file row (transforms copy)))))
      (map-rows copy :process-row-fn row-fn))))

(defun task-count (concurrency)
  "Return how many threads we are going to start given a number of WORKERS."
  (+ 1 concurrency concurrency))

(defmethod copy-from ((copy copy)
                      &key
                        (kernel nil k-s-p)
                        (channel nil c-s-p)
                        (worker-count 8)
                        (concurrency 2)
                        (on-error-stop *on-error-stop*)
                        disable-triggers)
  "Copy data from COPY source into PostgreSQL.

   We allow WORKER-COUNT simultaneous workers to be active at the same time
   in the context of this COPY object. A single unit of work consist of
   several kinds of workers:

     - a reader getting raw data from the COPY source with `map-rows',
     - N transformers preparing raw data for PostgreSQL COPY protocol,
     - N writers sending the data down to PostgreSQL.

   The N here is setup to the CONCURRENCY parameter: with a CONCURRENCY of
   2, we start (+ 1 2 2) = 5 concurrent tasks, with a CONCURRENCY of 4 we
   start (+ 1 4 4) = 9 concurrent tasks, of which only WORKER-COUNT may be
   active simultaneously."
  (let* ((table-name   (format-table-name (target copy)))
         (lp:*kernel*  (or kernel (make-kernel worker-count)))
         (channel      (or channel (lp:make-channel)))
         ;; Now, prepare data queues for workers:
         ;;   reader -> transformers -> writers
         (rawqs       (loop :repeat concurrency :collect
                         (lq:make-queue :fixed-capacity *concurrent-batches*)))
         (fmtqs       (loop :repeat concurrency :collect
                         (lq:make-queue :fixed-capacity *concurrent-batches*))))

    (with-stats-collection ((target copy) :dbname (db-name (target-db copy)))
        (lp:task-handler-bind
            ((error #'(lambda (condition)
                        (log-message :error "A thread failed with error: ~a"
                                     condition)
                        #-pgloader-image
                        (if (member *client-min-messages* (list :debug :data))
                            (lp::invoke-debugger condition)
                            (lp::invoke-transfer-error condition))
                        #+pgloader-image
                        (if (member *client-min-messages* (list :debug :data))
                            (log-message :fatal "Backtrace: ~a"
                                         (trivial-backtrace:print-backtrace
                                          condition
                                          :output nil
                                          :verbose t))
                            (lp::invoke-transfer-error condition)))))
          (log-message :notice "COPY ~s" table-name)

          ;; start a task to read data from the source into the queue
          (lp:submit-task channel #'queue-raw-data copy rawqs)

          ;; now start transformer threads to process raw vectors from our
          ;; source into preprocessed batches to send down to PostgreSQL
          (loop :for rawq :in rawqs
             :for fmtq :in fmtqs
             :do (lp:submit-task channel #'format-data-to-copy copy rawq fmtq))

          (loop :for fmtq :in fmtqs
             :do (lp:submit-task channel
                                 #'pgloader.pgsql:copy-from-queue
                                 (clone-connection (target-db copy))
                                 (target copy)
                                 fmtq
                                 :columns (copy-column-list copy)
                                 :on-error-stop on-error-stop
                                 :disable-triggers disable-triggers))

          ;; now wait until both the tasks are over, and kill the kernel
          (unless c-s-p
            (log-message :debug "waiting for ~d tasks" (task-count concurrency))
            (loop :repeat (task-count concurrency)
               :do (lp:receive-result channel))
            (log-message :notice "COPY ~s done." table-name)
            (unless k-s-p (lp:end-kernel :wait t)))))))

;;;
;;; Generic API for pgloader sources
;;;
(in-package :pgloader.load)

;;;
;;; Common API implementation
;;;
(defmethod queue-raw-data ((copy copy) rawq concurrency)
  "Stream data as read by the map-queue method on the COPY argument into QUEUE,
   as given."
  (log-message :notice "COPY ~a ~@[with ~d rows estimated~] [~a/~a]"
               (format-table-name (target copy))
               (table-row-count-estimate (target copy))
               (lp:kernel-worker-index)
               (lp:kernel-worker-count))
  (log-message :debug "Reader started for ~a" (format-table-name (target copy)))
  (let* ((start-time (get-internal-real-time))
         (row-count 0)
         (process-row
          (if (or (eq :data *log-min-messages*)
                  (eq :data *client-min-messages*))
              ;; when debugging, use a lambda with debug traces
              (lambda (row)
                (log-message :data "< ~s" row)
                (lq:push-queue row rawq)
                (incf row-count))

              ;; usual non-debug case
              (lambda (row)
                (lq:push-queue row rawq)
                (incf row-count)))))

    ;; signal we are starting
    (update-stats :data (target copy) :start start-time)

    ;; call the source-specific method for reading input data
    (map-rows copy :process-row-fn process-row)

    ;; process last batches and send them to queues
    ;; and mark end of stream
    (loop :repeat concurrency :do (lq:push-queue :end-of-data rawq))

    (let ((seconds (elapsed-time-since start-time)))
      (log-message :debug "Reader for ~a is done in ~6$s"
                   (format-table-name (target copy)) seconds)
      (update-stats :data (target copy) :read row-count :rs seconds)
      (list :reader (target copy) seconds))))


(defmethod copy-to ((copy copy) pgsql-copy-filename)
  "Extract data from COPY file into a PotgreSQL COPY TEXT formated file"
  (with-open-file (text-file pgsql-copy-filename
                             :direction :output
                             :if-exists :supersede
                             :external-format :utf-8)
    (let ((row-fn (lambda (row)
                    (format-vector-row text-file row (transforms copy)))))
      (map-rows copy :process-row-fn row-fn))))

(defmethod copy-from ((copy copy)
                      &key
                        (kernel nil k-s-p)
                        (channel nil c-s-p)
                        (worker-count 8)
                        (concurrency 2)
                        (multiple-readers nil)
                        (on-error-stop *on-error-stop*)
                        disable-triggers)
  "Copy data from COPY source into PostgreSQL."
  (let* ((table-name   (format-table-name (target copy)))
         (lp:*kernel*  (or kernel (make-kernel worker-count)))
         (channel      (or channel (lp:make-channel)))
         (readers      nil)
         (task-count   0))

    (flet ((submit-task (channel function &rest args)
             (apply #'lp:submit-task channel function args)
             (incf task-count)))

      (lp:task-handler-bind
          (#+pgloader-image
           (copy-init-error
            #'(lambda (condition)
                ;; stop the other tasks and then transfer the control
                (log-message :log "COPY INIT ERROR")
                (lp:invoke-transfer-error condition)))
           (on-error-stop
            #'(lambda (condition)
                (log-message :log "ON ERROR STOP")
                (lp:kill-tasks :default)
                (lp:invoke-transfer-error condition)))
           #+pgloader-image
           (error
            #'(lambda (condition)
                (log-message :error "A thread failed with error: ~a" condition)
                (log-message :error "~a"
                             (trivial-backtrace:print-backtrace condition
                                                                :output nil))
                (lp::invoke-transfer-error condition))))

        ;; Check for Read Concurrency Support from our source
        (when (and multiple-readers (< 1 concurrency))
          (let ((label "Check Concurrency Support"))
            (with-stats-collection (label :section :pre)
              (setf readers (concurrency-support copy concurrency))
              (update-stats :pre label :read 1 :rows (if readers 1 0))
              (when readers
                (log-message :notice "Multiple Readers Enabled for ~a"
                             (format-table-name (target copy)))))))

        ;; when reader is non-nil, we have reader concurrency support!
        (if readers
            ;; here we have detected Concurrency Support: we create as many
            ;; readers as writers and create associated couples, each couple
            ;; shares its own queue
            (let ((rawqs
                   (loop :repeat concurrency :collect
                      (lq:make-queue :fixed-capacity *prefetch-rows*))))
              (log-message :info "Read Concurrency Enabled for ~s"
                           (format-table-name (target copy)))

              (loop :for rawq :in rawqs :for reader :in readers :do
                 ;; each reader pretends to be alone, pass 1 as concurrency
                 (submit-task channel #'queue-raw-data reader rawq 1)

                 (submit-task channel #'copy-rows-from-queue
                              copy rawq
                              :on-error-stop on-error-stop
                              :disable-triggers disable-triggers)))

            ;; no Read Concurrency Support detected, start a single reader
            ;; task, using a single data queue that is read by multiple
            ;; writers.
            (let ((rawq
                   (lq:make-queue :fixed-capacity *prefetch-rows*)))
              (submit-task channel #'queue-raw-data copy rawq concurrency)

              ;; start a task to transform the raw data in the copy format
              ;; and send that data down to PostgreSQL
              (loop :repeat concurrency :do
                 (submit-task channel #'copy-rows-from-queue
                              copy rawq
                              :on-error-stop on-error-stop
                              :disable-triggers disable-triggers))))

        ;; now wait until both the tasks are over, and kill the kernel
        (unless c-s-p
          (log-message :debug "waiting for ~d tasks" task-count)
          (loop :repeat task-count :do (lp:receive-result channel))
          (log-message :notice "COPY ~s done." table-name)
          (unless k-s-p (lp:end-kernel :wait t)))

        ;; return task-count, which is how many tasks we submitted to our
        ;; lparallel kernel.
        task-count))))

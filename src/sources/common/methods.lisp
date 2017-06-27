;;;
;;; Generic API for pgloader sources
;;;
(in-package :pgloader.sources)

;;;
;;; Common API implementation
;;;
(defmethod queue-raw-data ((copy copy) rawq concurrency)
  "Stream data as read by the map-queue method on the COPY argument into QUEUE,
   as given."
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

(defmethod data-is-preformatted-p ((copy copy))
  "By default, data is not preformatted."
  nil)

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
  ;; (+ 1 concurrency concurrency)
  (+ 1 concurrency))

(defmethod copy-from ((copy copy)
                      &key
                        (kernel nil k-s-p)
                        (channel nil c-s-p)
                        (worker-count 8)
                        (concurrency 2)
                        (on-error-stop *on-error-stop*)
                        disable-triggers)
  "Copy data from COPY source into PostgreSQL."
  (let* ((table-name   (format-table-name (target copy)))
         (lp:*kernel*  (or kernel (make-kernel worker-count)))
         (channel      (or channel (lp:make-channel)))
         (rawq        (lq:make-queue :fixed-capacity *concurrent-batches*)))

    (lp:task-handler-bind
        ((on-error-stop
          #'(lambda (condition)
              ;; everything has been handled already
              (lp:invoke-transfer-error condition)))
         (error
          #'(lambda (condition)
              (log-message :error "A thread failed with error: ~a" condition)
              (if (member *client-min-messages* (list :debug :data))
                  #-pgloader-image
                  (log-message :error "~a"
                               (trivial-backtrace:print-backtrace condition
                                                                  :output nil))
                  #+pgloader-image
                  (lp::invoke-debugger condition))
              (lp::invoke-transfer-error condition))))
      (log-message :notice "COPY ~a" table-name)

      ;; start a task to read data from the source into the queue
      (lp:submit-task channel #'queue-raw-data copy rawq concurrency)

      ;; start a task to transform the raw data in the copy format
      ;; and send that data down to PostgreSQL
      (loop :repeat concurrency
         :do (lp:submit-task channel #'pgloader.pgsql::copy-rows-from-queue
                             copy rawq
                             :on-error-stop on-error-stop
                             :disable-triggers disable-triggers))

      ;; now wait until both the tasks are over, and kill the kernel
      (unless c-s-p
        (log-message :debug "waiting for ~d tasks" (task-count concurrency))
        (loop :repeat (task-count concurrency)
           :do (lp:receive-result channel))
        (log-message :notice "COPY ~s done." table-name)
        (unless k-s-p (lp:end-kernel :wait t))))))

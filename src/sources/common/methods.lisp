;;;
;;; Generic API for pgloader sources
;;;
(in-package :pgloader.sources)

;;;
;;; Common API implementation
;;;
(defmethod queue-raw-data ((copy copy) queue)
  "Stream data as read by the map-queue method on the COPY argument into QUEUE,
   as given."
  (log-message :debug "Reader started for ~a" (target copy))
  (let ((start-time (get-internal-real-time)))
    (map-rows copy :process-row-fn (lambda (data)
                                     (when (or (eq :data *log-min-messages*)
                                               (eq :data *client-min-messages*))
                                       (log-message :data "< ~s" data))
                                     (lq:push-queue data queue)))
    (lq:push-queue :end-of-data queue)

    (let ((seconds (elapsed-time-since start-time)))
     (log-message :info "Reader for ~a is done in ~6$s" (target copy) seconds)
     (list :reader (target copy) seconds))))

(defmethod format-data-to-copy ((copy copy) raw-queue formatted-queue
                                &optional pre-formatted)
  "Loop over the data in the RAW-QUEUE and prepare it in batches in the
   FORMATED-QUEUE, ready to be sent down to PostgreSQL using the COPY protocol."
  (log-message :debug "Transformer in action for ~a!" (target copy))
  (let ((start-time (get-internal-real-time)))

    (pgloader.queue:cook-batches copy raw-queue formatted-queue pre-formatted)

    ;; and return
    (let ((seconds (elapsed-time-since start-time)))
     (log-message :info "Transformer for ~a is done in ~6$s" (target copy) seconds)
     (list :worker (target copy) seconds))))

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

(defmethod copy-from ((copy copy)
                      &key
                        (kernel nil k-s-p)
                        (channel nil c-s-p)
                        truncate
                        disable-triggers)
  "Copy data from COPY source into PostgreSQL."
  (let* ((lp:*kernel* (or kernel (make-kernel 4)))
         (channel     (or channel (lp:make-channel)))
         (rawq        (lq:make-queue))
         (fmtq        (lq:make-queue :fixed-capacity *concurrent-batches*))
         (table-name  (format-table-name (target copy))))

    (with-stats-collection ((target copy) :dbname (db-name (target-db copy)))
        (lp:task-handler-bind () ;; ((error #'lp:invoke-transfer-error))
          (log-message :info "COPY ~s" table-name)

          ;; start a tast to read data from the source into the queue
          (lp:submit-task channel #'queue-raw-data copy rawq)

          ;; now start a transformer thread to process raw vectors from our
          ;; source into preprocessed batches to send down to PostgreSQL
          (lp:submit-task channel #'format-data-to-copy copy rawq fmtq)

          ;; And start two tasks to push that data from the queue into
          ;; PostgreSQL; Andres Freund research/benchmarks show that in every
          ;; PostgreSQL releases up to 9.5 included the highest throughput of
          ;; COPY TO the same table is achieved with 2 concurrent clients...
          ;;
          ;; See Extension Lock Scalability slide in
          ;; http://www.anarazel.de/talks/pgconf-eu-2015-10-30/concurrency.pdf
          ;;
          ;; Let's just hardcode 2 threads for that then.
          (loop :for w :below 2
             :do (lp:submit-task channel
                                 #'pgloader.pgsql:copy-from-queue
                                 (clone-connection (target-db copy))
                                 (target copy)
                                 fmtq
                                 :columns (copy-column-list copy)
                                 :truncate truncate
                                 :disable-triggers disable-triggers))

          ;; now wait until both the tasks are over, and kill the kernel
          (unless c-s-p
            (loop :repeat (lp:kernel-worker-count)
               :do (lp:receive-result channel)
               :finally (progn (log-message :info "COPY ~s done." table-name)
                               (unless k-s-p (lp:end-kernel)))))))))

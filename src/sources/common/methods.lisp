;;;
;;; Generic API for pgloader sources
;;;
(in-package :pgloader.sources)

;;;
;;; Common API implementation
;;;
(defmethod copy-to-queue ((copy copy) queue)
  "Copy data from given COPY definition into lparallel.queue QUEUE"
  (pgloader.queue:map-push-queue copy queue))


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
                        truncate
                        disable-triggers)
  "Copy data from COPY source into PostgreSQL."
  (let* ((lp:*kernel* (or kernel (make-kernel 2)))
         (channel     (lp:make-channel))
         (queue       (lq:make-queue :fixed-capacity *concurrent-batches*))
         (table-name  (format-table-name (target copy))))

    (with-stats-collection ((target copy) :dbname (db-name (target-db copy)))
        (lp:task-handler-bind ((error #'lp:invoke-transfer-error))
          (log-message :notice "COPY ~s" table-name)

          ;; start a tast to read data from the source into the queue
          (lp:submit-task channel #'copy-to-queue copy queue)

          ;; and start another task to push that data from the queue into
          ;; PostgreSQL
          (lp:submit-task channel
                          #'pgloader.pgsql:copy-from-queue
                          (target-db copy)
                          (target copy)
                          queue
                          :columns (copy-column-list copy)
                          :truncate truncate
                          :disable-triggers disable-triggers)

          ;; now wait until both the tasks are over, and kill the kernel
          (loop :for tasks :below 2 :do (lp:receive-result channel)
             :finally
             (log-message :info "COPY ~s done." table-name)
             (unless k-s-p (lp:end-kernel)))))))

;;;
;;; The PostgreSQL COPY TO implementation, with batches and retries.
;;;
(in-package :pgloader.pgcopy)

(defun batch-rows-to-copy (table columns copy nbcols queue)
  "Add rows that we pop from QUEUE into a batch, that we then COPY over to
   PostgreSQL as soon as the batch is full. This allows sophisticated error
   handling and recovery, where we can retry rows that are not rejected by
   PostgreSQL."
  (let ((seconds       0)
        (current-batch (make-batch)))
    (loop
       :for row := (lq:pop-queue queue)
       :until (eq :end-of-data row)
       :do (multiple-value-bind (maybe-new-batch seconds-in-this-batch)
               (add-row-to-current-batch table columns copy nbcols
                                         current-batch row
                                         :send-batch-fn (function send-batch)
                                         :format-row-fn #'prepare-and-format-row)
             (setf current-batch maybe-new-batch)
             (incf seconds seconds-in-this-batch)))

    ;; the last batch might not be empty
    (unless (= 0 (batch-count current-batch))
      (incf seconds (send-batch table columns current-batch)))

    seconds))


(defun send-batch (table columns batch &key (db pomo:*database*))
  "Copy current *writer-batch* into TABLE-NAME."
  ;; We need to keep a copy of the rows we send through the COPY
  ;; protocol to PostgreSQL to be able to process them again in case
  ;; of a data error being signaled, that's the BATCH here.
  (let ((batch-start-time (get-internal-real-time))
        (table-name       (format-table-name table))
        (pomo:*database*  db))
    ;; We can't use with-pgsql-transaction here because of the specifics
    ;; of error handling in case of cl-postgres:open-db-writer errors: the
    ;; transaction is dead already when we get a signal, and the COMMIT or
    ;; ABORT steps then trigger a protocol error on a #\Z message.
    (handler-case
        (progn
          (pomo:execute "BEGIN")
          (let* ((copier
                  (handler-case
                      (cl-postgres:open-db-writer db table-name columns)
                    (condition (c)
                      ;; failed to open the COPY protocol mode (e.g. missing
                      ;; columns on the target table), stop here,
                      ;; transaction is dead already (no ROLLBACK needed).
                      (error (make-condition 'copy-init-error
                                             :table table
                                             :columns columns
                                             :condition c))))))
            (unwind-protect
                 (db-write-batch copier batch)
              (cl-postgres:close-db-writer copier)
              (pomo:execute "COMMIT"))))

      ;; If PostgreSQL signals a data error, process the batch by isolating
      ;; erroneous data away and retrying the rest.
      (postgresql-retryable (condition)
        (pomo:execute "ROLLBACK")

        (log-message :error "PostgreSQL [~s] ~a" table-name condition)
        ;; clean the current transaction before retrying new ones
        (let ((errors
               (handler-case
                   (retry-batch table columns batch condition)
                 (condition (e)
                   (log-message :error "BUG: failed to retry-batch: ~a" e)
                   (batch-count batch)))))
          (log-message :debug "retry-batch found ~d errors" errors)
          (update-stats :data table :errs errors :rows (- errors))))

      (postgresql-unavailable (condition)

        (log-message :error "[PostgreSQL ~s] ~a" table-name condition)
        (log-message :error "Copy Batch reconnecting to PostgreSQL")

        ;; in order to avoid Socket error in "connect": ECONNREFUSED if we
        ;; try just too soon, wait a little
        (sleep 2)

        (cl-postgres:reopen-database db)
        (send-batch table columns batch :db db))

      (copy-init-error (condition)
        ;; Couldn't init the COPY protocol, process the condition up the
        ;; stack
        (update-stats :data table :errs 1)
        (error condition))

      (condition (c)
        ;; non retryable failures
        (log-message :error "Non-retryable error ~a" c)
        (pomo:execute "ROLLBACK")))

    ;; now log about having send a batch, and update our stats with the
    ;; time that took
    (let ((seconds (elapsed-time-since batch-start-time)))
      (log-message :debug
                   "send-batch[~a] ~a ~d row~:p [~a] in ~6$s~@[ [oversized]~]"
                   (lp:kernel-worker-index)
                   (format-table-name table)
                   (batch-count batch)
                   (pretty-print-bytes (batch-bytes batch))
                   seconds
                   (batch-oversized-p batch))
      (update-stats :data table
                    :rows (batch-count batch)
                    :bytes (batch-bytes batch))

      ;; and return batch-seconds
      seconds)))


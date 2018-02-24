;;;
;;; The PostgreSQL COPY TO implementation, with batches and retries.
;;;
(in-package :pgloader.pgcopy)

(defun stream-rows-to-copy (table columns copy nbcols queue
                            &optional (db pomo:*database*))
  "Directly stream rows that we pop from QUEUE into PostgreSQL database
   connection DB."
  (let ((rcount  0)
        (bytes   0)
        (seconds 0))
    (handler-case
        (progn
          (pomo:execute "BEGIN")
          (let* ((table-name (format-table-name table))
                 (copier
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
                 (loop
                    :for row := (lq:pop-queue queue)
                    :until (eq :end-of-data row)
                    :do (multiple-value-bind (row-bytes row-seconds)
                            (stream-row copier copy nbcols row)
                          (incf rcount)
                          (incf bytes row-bytes)
                          (incf seconds row-seconds)))
              (cl-postgres:close-db-writer copier)
              (pomo:execute "COMMIT"))))

      (postgresql-unavailable (condition)
        ;; We got disconnected, maybe because PostgreSQL is being restarted,
        ;; maybe for another reason, but in any case the transaction doesn't
        ;; exists anymore, the connection doesn't exists anymore, there's no
        ;; need to send anything, not even a ROLLBACK; in that case.
        ;;
        ;; Re-signal the condition as an error to be processed by the calling
        ;; thread, where it's possible to also stop the reader.
        (error condition))

      (copy-init-error (condition)
        (update-stats :data table :errs 1)
        (error condition))

      (condition (c)
        ;; stop at any failure here, this function doesn't implement any kind
        ;; of retry behaviour.
        (log-message :error "~a" c)
        (update-stats :data table :errs 1)
        (pomo:execute "ROLLBACK")
        (return-from stream-rows-to-copy seconds)))

    ;; return seconds spent sending data to PostgreSQL
    (update-stats :data table :rows rcount :bytes bytes)
    seconds))

(defun stream-row (stream copy nbcols row)
  "Send a single ROW down in the PostgreSQL COPY STREAM."
  (let* ((start (get-internal-real-time))
         (row   (prepare-row copy nbcols row)))
    (when row
      (let ((bytes
             (ecase (copy-format copy)
               (:raw      (db-write-vector-row stream row nbcols))
               (:escaped  (db-write-escaped-vector-row stream row nbcols)))))
        (values bytes (elapsed-time-since start))))))

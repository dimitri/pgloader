;;;
;;; The PostgreSQL COPY TO implementation, with batches and retries.
;;;
(in-package :pgloader.copy)

(defun send-batch (table columns batch
                   &key
                     (db pomo:*database*)
                     on-error-stop)
  "Copy current *writer-batch* into TABLE-NAME."
  ;; We need to keep a copy of the rows we send through the COPY
  ;; protocol to PostgreSQL to be able to process them again in case
  ;; of a data error being signaled, that's the BATCH here.
  (let ((table-name (format-table-name table))
        (pomo:*database* db))
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
                      (log-message :fatal
                                   "Can't init COPY to ~a~@[(~{~a~^, ~})~]: ~%~a"
                                   (format-table-name table)
                                   columns
                                   c)
                      (update-stats :data table :errs 1)
                      (return-from send-batch 0)))))
            (unwind-protect
                 (db-write-batch copier batch)
              (cl-postgres:close-db-writer copier)
              (pomo:execute "COMMIT"))))

      ;; If PostgreSQL signals a data error, process the batch by isolating
      ;; erroneous data away and retrying the rest.
      (postgresql-retryable (condition)
        (pomo:execute "ROLLBACK")

        (log-message :error "PostgreSQL [~s] ~a" table-name condition)
        (if on-error-stop
            ;; re-signal the condition to upper level
            (signal 'on-error-stop :on-condition condition)

            ;; normal behavior, on-error-stop being nil
            ;; clean the current transaction before retrying new ones
            (let ((errors
                   (retry-batch table columns batch condition)))
              (log-message :debug "retry-batch found ~d errors" errors)
              (update-stats :data table :rows (- errors)))))

      (postgresql-unavailable (condition)

        (log-message :error "[PostgreSQL ~s] ~a" table-name condition)
        (log-message :error "Copy Batch reconnecting to PostgreSQL")

        ;; in order to avoid Socket error in "connect": ECONNREFUSED if we
        ;; try just too soon, wait a little
        (sleep 2)

        (cl-postgres:reopen-database db)
        (send-batch table columns batch :db db :on-error-stop on-error-stop))

      (condition (c)
        ;; non retryable failures
        (log-message :error "Non-retryable error ~a" c)
        (pomo:execute "ROLLBACK")))))

;;;
;;; We receive raw input rows from an lparallel queue, push their content
;;; down to PostgreSQL, handling any data related errors in the way.
;;;
(defun copy-rows-from-queue (copy queue
                             &key
                               disable-triggers
                               on-error-stop
                               (columns
                                (pgloader.sources:copy-column-list copy))
                             &aux
                               (pgconn  (clone-connection
                                         (pgloader.sources:target-db copy)))
                               (table   (pgloader.sources:target copy)))
  "Fetch rows from the QUEUE, prepare them in batches and send them down to
   PostgreSQL, and when that's done update stats."
  (let* ((nbcols        (length
                         (table-column-list (pgloader.sources::target copy))))
         (current-batch (make-batch))
         (seconds 0))

    ;; add some COPY activity related bits to our COPY object.
    (setf (transforms copy)
          (let ((funs (transforms copy)))
            (unless (every #'null funs)
              funs))

          ;; FIXME: we should change the API around preprocess-row, someday.
          (preprocessor copy)
          (pgloader.sources::preprocess-row copy)

          ;; FIXME: we could change the API around data-is-preformatted-p,
          ;; but that's a bigger change than duplicating the information in
          ;; the object.
          (copy-format copy)
          (if (data-is-preformatted-p copy) :escaped :raw))

    (flet ((send-current-batch (unqualified-table-name)
             ;; we close over the whole lexical environment or almost...
             (let ((batch-start-time (get-internal-real-time)))
               (send-batch table
                           columns
                           current-batch
                           :on-error-stop on-error-stop)

               (let ((batch-seconds (elapsed-time-since batch-start-time)))
                 (log-message :debug
                              "send-batch[~a] ~a ~d row~:p [~a] in ~6$s~@[ [oversized]~]"
                              (lp:kernel-worker-index)
                              unqualified-table-name
                              (batch-count current-batch)
                              (pretty-print-bytes (batch-bytes current-batch))
                              batch-seconds
                              (batch-oversized-p current-batch))
                 (update-stats :data table
                               :rows (batch-count current-batch)
                               :bytes (batch-bytes current-batch))

                 ;; return batch-seconds
                 batch-seconds))))
      (declare (inline send-current-batch))

      (pgloader.pgsql:with-pgsql-connection (pgconn)
        (with-schema (unqualified-table-name table)
          (with-disabled-triggers (unqualified-table-name
                                   :disable-triggers disable-triggers)
            (log-message :info "pgsql:copy-rows-from-queue[~a]: ~a ~a"
                         (lp:kernel-worker-index)
                         (format-table-name table)
                         columns)

            (loop
               :for row := (lq:pop-queue queue)
               :until (eq :end-of-data row)
               :do
               (progn
                 ;; if current-batch is full, send data to PostgreSQL
                 ;; and prepare a new batch
                 (when (batch-full-p current-batch)
                   (incf seconds (send-current-batch unqualified-table-name))
                   (setf current-batch (make-batch))

                   ;; give a little help to our friend, now is a good time
                   ;; to garbage collect
                   #+sbcl (sb-ext:gc :full t))

                 ;; also add up the time it takes to format the rows
                 (let ((start-time (get-internal-real-time)))
                   (format-row-in-batch copy nbcols row current-batch)
                   (incf seconds (elapsed-time-since start-time)))))

            ;; the last batch might not be empty
            (unless (= 0 (batch-count current-batch))
              (incf seconds (send-current-batch unqualified-table-name)))))))

    ;; each writer thread sends its own stop timestamp and the monitor keeps
    ;; only the latest entry
    (update-stats :data table :ws seconds :stop (get-internal-real-time))
    (log-message :debug "Writer[~a] for ~a is done in ~6$s"
                 (lp:kernel-worker-index)
                 (format-table-name table)
                 seconds)
    (list :writer table seconds)))


(defun format-row-in-batch (copy nbcols row current-batch)
  "Given a row from the queue, prepare it for the next batch."
  (let* ((row               (if (preprocessor copy)
                                (funcall (preprocessor copy) row)
                                row))
         (transformed-row   (cond ((eq :escaped (copy-format copy)) row)
                                  ((null (transforms copy))         row)
                                  (t
                                   (apply-transforms copy
                                                     nbcols
                                                     row
                                                     (transforms copy))))))
    (multiple-value-bind (pg-vector-row bytes)
        (if transformed-row
            (ecase (copy-format copy)
              (:raw     (format-vector-row nbcols transformed-row))
              (:escaped (format-escaped-vector-row nbcols transformed-row)))
            (values nil 0))

      ;; we might have to debug
      (when pg-vector-row
        (log-message :data "> ~s" (map 'string #'code-char pg-vector-row))

        ;; now add copy-data to current-batch
        (push-row current-batch pg-vector-row bytes)))))

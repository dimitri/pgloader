;;;
;;; The PostgreSQL COPY TO implementation, with batches and retries.
;;;
(in-package :pgloader.pgsql)

;;;
;;; Stream prepared data from *writer-batch* down to PostgreSQL using the
;;; COPY protocol, and retry the batch avoiding known bad rows (from parsing
;;; COPY error messages) in case some data related conditions are signaled.
;;;
(defun db-write-row (copier data)
  "Copy cl-postgres:db-write-row guts to avoid computing utf-8 bytes all
   over again, as we reproduced the data formating in pgloader code. The
   reason we do that is to be able to lower the cost of retrying batches:
   the formating has then already been done."
  (let* ((connection          (cl-postgres::copier-database copier))
	 (cl-postgres::socket (cl-postgres::connection-socket connection)))
    (cl-postgres::with-reconnect-restart connection
      (cl-postgres::using-connection connection
        (cl-postgres::with-syncing
          (cl-postgres::write-uint1 cl-postgres::socket 100)
          (cl-postgres::write-uint4 cl-postgres::socket (+ 4 (length data)))
          (loop :for byte :across data
             :do (write-byte byte cl-postgres::socket))))))
  (incf (cl-postgres::copier-count copier)))

(defun copy-batch (table columns batch batch-rows
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
                      (return-from copy-batch 0)))))
            (unwind-protect
                 (loop :for i :below batch-rows
                    :for data := (aref batch i)
                    :do (when data
                          (db-write-row copier data))
                    :finally (return batch-rows))
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
                   (retry-batch table columns batch batch-rows condition)))
              (log-message :debug "retry-batch found ~d errors" errors)
              (update-stats :data table :rows (- errors)))))

      (postgresql-unavailable (condition)

        (log-message :error "[PostgreSQL ~s] ~a" table-name condition)
        (log-message :error "Copy Batch reconnecting to PostgreSQL")

       ;; in order to avoid Socket error in "connect": ECONNREFUSED if we
       ;; try just too soon, wait a little
       (sleep 2)

        (cl-postgres:reopen-database db)
        (copy-batch table columns batch batch-rows
                    :db db
                    :on-error-stop on-error-stop))

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
  (let ((preprocessor  (pgloader.sources::preprocess-row copy))
        (pre-formatted (pgloader.sources:data-is-preformatted-p copy))
        (current-batch (make-batch))
        (seconds 0))

    (flet ((send-current-batch (unqualified-table-name)
             ;; we close over the whole lexical environment or almost...
             (let ((batch-start-time (get-internal-real-time)))
               (copy-batch table
                           columns
                           (batch-data current-batch)
                           (batch-count current-batch)
                           :on-error-stop on-error-stop)

               (let ((batch-seconds (elapsed-time-since batch-start-time)))
                 (log-message :debug
                              "copy-batch[~a] ~a ~d row~:p [~a] in ~6$s~@[ [oversized]~]"
                              (lp:kernel-worker-index)
                              unqualified-table-name
                              (batch-count current-batch)
                              (pretty-print-bytes (batch-bytes current-batch))
                              batch-seconds
                              (batch-oversized-p current-batch))
                 (update-stats :data table :rows (batch-count current-batch))

                 ;; return batch-seconds
                 batch-seconds))))
      (declare (inline send-current-batch))

      (with-pgsql-connection (pgconn)
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
                   (let ((batch-seconds
                          (send-current-batch unqualified-table-name)))
                     (incf seconds batch-seconds))
                   (setf current-batch (make-batch)))

                 (format-row-in-batch copy row current-batch
                                      preprocessor pre-formatted)))

            ;; the last batch might not be empty
            (unless (= 0 (batch-count current-batch))
              (send-current-batch unqualified-table-name))))))

    ;; each writer thread sends its own stop timestamp and the monitor keeps
    ;; only the latest entry
    (update-stats :data table :ws seconds :stop (get-internal-real-time))
    (log-message :debug "Writer[~a] for ~a is done in ~6$s"
                 (lp:kernel-worker-index)
                 (format-table-name table)
                 seconds)
    (list :writer table seconds)))


(declaim (inline send-current-batch))
(defun format-row-in-batch (copy row current-batch preprocessor pre-formatted)
  "Given a row from the queue, prepare it for the next batch."
  (metabang.bind:bind
      ((row           (if preprocessor (funcall preprocessor row) row))

       ((:values copy-data bytes)
        (handler-case
            (format-vector-row row
                               (pgloader.sources::transforms copy)
                               pre-formatted)
          (condition (e)
            (log-message :error "Error while formating a row from ~s:"
                         (format-table-name (pgloader.sources:target copy)))
            (log-message :error "~a" e)
            (update-stats :data (pgloader.sources:target copy) :errs 1)
            (values nil 0)))))
    ;; we might have to debug
    (when copy-data
      (log-message :data "> ~s"
                   (map 'string #'code-char copy-data)))

    ;; now add copy-data to current-batch
    (push-row current-batch copy-data bytes)))

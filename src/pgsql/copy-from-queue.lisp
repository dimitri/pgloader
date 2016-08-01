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
  (let ((pomo:*database* db))
    (handling-pgsql-notices
      ;; We can't use with-pgsql-transaction here because of the specifics
      ;; of error handling in case of cl-postgres:open-db-writer errors: the
      ;; transaction is dead already when we get a signal, and the COMMIT or
      ;; ABORT steps then trigger a protocol error on a #\Z message.
      (pomo:execute "BEGIN")
      (handler-case
          (let* ((table-name (format-table-name table))
                 (copier
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
              (pomo:execute "COMMIT")))

        ;; If PostgreSQL signals a data error, process the batch by isolating
        ;; erroneous data away and retrying the rest.
        ((or
          cl-postgres-error::data-exception
          cl-postgres-error::integrity-violation
          cl-postgres-error::internal-error
          cl-postgres-error::insufficient-resources
          cl-postgres-error::program-limit-exceeded) (condition)

          (pomo:execute "ROLLBACK")

          (if on-error-stop
              ;; re-signal the condition to upper level
              (progn
                (log-message :fatal "~a" condition)
                (error "Stop loading data for table ~s on first error."
                       (format-table-name table)))

              ;; normal behavior, on-error-stop being nil
              ;; clean the current transaction before retrying new ones
              (progn
                (log-message :error "~a" condition)
                (retry-batch table columns batch batch-rows condition))))

        (condition (c)
          ;; non retryable failures
          (log-message :error "Non-retryable error ~a" c)
          (pomo:execute "ROLLBACK"))))))

;;;
;;; We receive fully prepared batch from an lparallel queue, push their
;;; content down to PostgreSQL, handling any data related errors in the way.
;;;
(defun copy-from-queue (pgconn table queue
			&key
                          columns
                          disable-triggers
                          on-error-stop)
  "Fetch from the QUEUE messages containing how many rows are in the
   *writer-batch* for us to send down to PostgreSQL, and when that's done
   update stats."
  (let ((seconds 0))
    (with-pgsql-connection (pgconn)
      (with-schema (unqualified-table-name table)
        (with-disabled-triggers (unqualified-table-name
                                 :disable-triggers disable-triggers)
          (log-message :info "pgsql:copy-from-queue[~a]: ~a ~a"
                       (lp:kernel-worker-index)
                       (format-table-name table)
                       columns)

          (loop
             :for (mesg batch read oversized?) := (lq:pop-queue queue)
             :until (eq :end-of-data mesg)
             :for (rows batch-seconds) :=
             (let ((start-time (get-internal-real-time)))
               (list (copy-batch table columns batch read
                                 :on-error-stop on-error-stop)
                     (elapsed-time-since start-time)))
             :do (progn
                   ;; The SBCL implementation needs some Garbage Collection
                   ;; decision making help... and now is a pretty good time.
                   #+sbcl (when oversized?
                            (log-message :debug "Forcing a full GC.")
                            (sb-ext:gc :full t))
                   (log-message :debug
                                "copy-batch[~a] ~a ~d row~:p in ~6$s~@[ [oversized]~]"
                                (lp:kernel-worker-index)
                                unqualified-table-name
                                rows
                                batch-seconds
                                oversized?)
                   (update-stats :data table :rows rows)
                   (incf seconds batch-seconds))))))

    (update-stats :data table :ws seconds)
    (log-message :debug "Writer[~a] for ~a is done in ~6$s"
                 (lp:kernel-worker-index)
                 (format-table-name table) seconds)
    (list :writer table seconds)))

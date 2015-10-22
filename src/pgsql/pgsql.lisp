;;;
;;; The PostgreSQL COPY TO implementation, with batches and retries.
;;;
(in-package :pgloader.pgsql)

;;;
;;; Stream prepared data from *writer-batch* down to PostgreSQL using the
;;; COPY protocol, and retry the batch avoiding known bad rows (from parsing
;;; COPY error messages) in case some data related conditions are signaled.
;;;
(defun copy-batch (table-name columns batch batch-rows
                   &key (db pomo:*database*))
  "Copy current *writer-batch* into TABLE-NAME."
  (handler-case
      (with-pgsql-transaction (:database db)
        ;; We need to keep a copy of the rows we send through the COPY
        ;; protocol to PostgreSQL to be able to process them again in case
        ;; of a data error being signaled, that's the BATCH here.
        (let ((copier (cl-postgres:open-db-writer db table-name columns)))
          (unwind-protect
               (loop for i below batch-rows
                  for copy-string = (aref batch i)
                  do (when (or (eq :data *log-min-messages*)
                               (eq :data *client-min-messages*))
                       (log-message :data "> ~s" copy-string))
                  do (cl-postgres:db-write-row copier nil copy-string)
                  finally (return batch-rows))
            (cl-postgres:close-db-writer copier))))

    ;; If PostgreSQL signals a data error, process the batch by isolating
    ;; erroneous data away and retrying the rest.
    ((or
      cl-postgres-error::data-exception
      cl-postgres-error::integrity-violation
      cl-postgres-error::internal-error
      cl-postgres-error::insufficient-resources
      cl-postgres-error::program-limit-exceeded) (condition)
      (retry-batch table-name columns batch batch-rows condition))))

;;;
;;; We receive fully prepared batch from an lparallel queue, push their
;;; content down to PostgreSQL, handling any data related errors in the way.
;;;
(defun copy-from-queue (pgconn table-name queue
			&key
                          columns
                          (truncate t)
                          disable-triggers)
  "Fetch from the QUEUE messages containing how many rows are in the
   *writer-batch* for us to send down to PostgreSQL, and when that's done
   update stats."
  (let ((start-time (get-internal-real-time)))
    (when truncate
      (truncate-tables pgconn (list table-name)))

    (with-pgsql-connection (pgconn)
      (with-schema (unqualified-table-name table-name)
        (with-disabled-triggers (unqualified-table-name
                                 :disable-triggers disable-triggers)
          (log-message :info "pgsql:copy-from-queue: ~a ~a" table-name columns)

          (loop
             :for (mesg batch read oversized?) := (lq:pop-queue queue)
             :until (eq mesg :end-of-data)
             :for rows := (copy-batch unqualified-table-name columns batch read)
             :do (progn
                   ;; The SBCL implementation needs some Garbage Collection
                   ;; decision making help... and now is a pretty good time.
                   #+sbcl (when oversized? (sb-ext:gc :full t))
                   (log-message :debug "copy-batch ~a ~d row~:p~:[~; [oversized]~]"
                                unqualified-table-name rows oversized?)
                   (update-stats :data table-name :rows rows))))))

    (let ((seconds (elapsed-time-since start-time)))
      (log-message :info "Writer for ~a is done in ~fs" table-name seconds)
      (update-stats :data table-name :ws seconds)
      (list :writer table-name seconds))))

;;;
;;; Compute how many rows we're going to try loading next, depending on
;;; where we are in the batch currently and where is the next-error to be
;;; seen, if that's between current position and the end of the batch.
;;;
(defun next-batch-rows (batch-rows current-batch-pos next-error)
  "How many rows should we process in next iteration?"
  (cond
    ((< current-batch-pos next-error)
     ;; We Can safely push a batch with all the rows until the first error,
     ;; and here current-batch-pos should be 0 anyways.
     ;;
     ;; How many rows do we have from position 0 to position next-error,
     ;; excluding next-error? Well, next-error.
     (- next-error current-batch-pos))

    ((= current-batch-pos next-error)
     ;; Now we got to the line that we know is an error, we need to process
     ;; only that one in the next batch
     1)

    (t
     ;; We're past the known erroneous row. The batch might have new errors,
     ;; or maybe that was the only one. We'll figure it out soon enough,
     ;; let's try the whole remaining rows.
     (- batch-rows current-batch-pos))))

;;;
;;; In case of COPY error, PostgreSQL gives us the line where the error was
;;; found as a CONTEXT message. Let's parse that information to optimize our
;;; batching splitting in case of errors.
;;;
;;;  CONTEXT: COPY errors, line 1, column b: "2006-13-11"
;;;  CONTEXT: COPY byte, line 1: "hello\0world"
;;;
;;; Those error messages are a translation target, tho, so we can only
;;; assume to recognize the command tag (COPY), the comma, and a numer after
;;; a world that might be Zeile (de), línea (es), ligne (fr), riga (it),
;;; linia (pl), linha (pt), строка (ru), 行 (zh), or something else
;;; entirely.
;;;
(defun parse-copy-error-context (context)
  "Given a COPY command CONTEXT error message, return the batch position
   where the error comes from."
  (cl-ppcre:register-groups-bind ((#'parse-integer n))
      ("COPY [^,]+, [^ ]+ (\\d+)" context :sharedp t)
    (1- n)))

;;;
;;; The main retry batch function.
;;;
(defun retry-batch (table-name columns batch batch-rows condition
                    &optional (current-batch-pos 0)
                    &aux (nb-errors 0))
  "Batch is a list of rows containing at least one bad row, the first such
   row is known to be located at FIRST-ERROR index in the BATCH array."

  (log-message :info "Entering error recovery.")

  (loop
     :with next-error = (parse-copy-error-context
                         (cl-postgres::database-error-context condition))

     :while (< current-batch-pos batch-rows)

     :do
     (progn                             ; indenting helper
       (when (= current-batch-pos next-error)
         (log-message :info "error recovery at ~d/~d, processing bad row"
                      (+ 1 next-error) batch-rows)
         (process-bad-row table-name condition (aref batch current-batch-pos))
         (incf current-batch-pos)
         (incf nb-errors))

       (let* ((current-batch-rows
               (next-batch-rows batch-rows current-batch-pos next-error)))
         (when (< 0 current-batch-rows)
          (handler-case
              (with-pgsql-transaction (:database pomo:*database*)
                (let* ((stream
                        (cl-postgres:open-db-writer pomo:*database*
                                                    table-name columns)))

                  (if (< current-batch-pos next-error)
                      (log-message :info
                                   "error recovery at ~d/~d, next error at ~d, loading ~d row~:p"
                                   current-batch-pos batch-rows (+ 1 next-error) current-batch-rows)
                      (log-message :info
                                   "error recovery at ~d/~d, trying ~d row~:p"
                                   current-batch-pos batch-rows current-batch-rows))

                  (unwind-protect
                       (loop :repeat current-batch-rows
                          :for pos :from current-batch-pos
                          :do (cl-postgres:db-write-row stream nil (aref batch pos)))

                    ;; close-db-writer is the one signaling cl-postgres-errors
                    (cl-postgres:close-db-writer stream)
                    (incf current-batch-pos current-batch-rows))))

            ;; the batch didn't make it, prepare error handling for next turn
            ((or
              cl-postgres-error::data-exception
              cl-postgres-error::integrity-violation
              cl-postgres-error:internal-error
              cl-postgres-error::insufficient-resources
              cl-postgres-error::program-limit-exceeded) (next-error-in-batch)

              (setf condition next-error-in-batch

                    next-error
                    (+ current-batch-pos
                       (parse-copy-error-context
                        (cl-postgres::database-error-context condition))))))))))

  (log-message :info "Recovery found ~d errors in ~d row~:p" nb-errors batch-rows)

  ;; Return how many rows we did load, for statistics purposes
  (- batch-rows nb-errors))

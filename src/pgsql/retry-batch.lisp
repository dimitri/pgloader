;;;
;;; The PostgreSQL COPY TO implementation, with batches and retries.
;;;
(in-package #:pgloader.pgsql)

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
(defun retry-batch (table columns batch batch-rows condition
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
         (process-bad-row table condition (aref batch current-batch-pos))
         (incf current-batch-pos)
         (incf nb-errors))

       (let* ((current-batch-rows
               (next-batch-rows batch-rows current-batch-pos next-error)))
         (when (< 0 current-batch-rows)
          (handler-case
              (with-pgsql-transaction (:database pomo:*database*)
                (let* ((table-name (format-table-name table))
                       (stream
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
                          :do (db-write-row stream (aref batch pos)))

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

;;;
;;; The PostgreSQL COPY TO implementation, with batches and retries.
;;;
(in-package #:pgloader.pgcopy)

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
;;; Binary-search recovery for batches where PostgreSQL does not supply a
;;; COPY line number in the error CONTEXT (e.g. FK violations, deferred
;;; constraint checks).
;;;
;;; The function tries to commit the first half of the range; on success it
;;; recurses into the second half.  On failure it recurses into the failing
;;; half.  At the base case (count = 1) it tries the single row and, if that
;;; fails too, rejects it via PROCESS-BAD-ROW.
;;;
;;; Complexity: O(k · log N) COPY round-trips for k bad rows in a batch of N.
;;;
(defun bisect-batch (table-name table columns batch start count)
  "Find and reject all bad rows in BATCH[START .. START+COUNT) via binary
   search.  Commits each good sub-range independently.  Returns the number
   of rows rejected."
  (if (<= count 1)
      ;; Base case: try this single row so we get the exact condition to log.
      (progn
        (log-message :debug "bisect: trying 1 row at position ~d" start)
        (handler-case
            (progn
              (copy-partial-batch table-name columns batch 1 start)
              0)
          (postgresql-retryable (c)
            (pomo:execute "ROLLBACK")
            (log-message :info "error recovery: rejecting row at position ~d" start)
            (log-message :error "PostgreSQL [~s] ~a" table-name c)
            (process-bad-row table c (aref (batch-data batch) start))
            1)))
      ;; Recursive case: split, try each half independently.
      (let* ((half   (floor count 2))
             (errors 0))
        ;; First half [start .. start+half)
        (log-message :debug "bisect: trying ~d rows [~d, ~d)" half start (+ start half))
        (incf errors
              (handler-case
                  (progn
                    (copy-partial-batch table-name columns batch half start)
                    0)
                (postgresql-retryable (c)
                  (pomo:execute "ROLLBACK")
                  (log-message :error "PostgreSQL [~s] ~a" table-name c)
                  (bisect-batch table-name table columns batch start half))))
        ;; Second half [start+half .. start+count)
        (log-message :debug "bisect: trying ~d rows [~d, ~d)"
                     (- count half) (+ start half) (+ start count))
        (incf errors
              (handler-case
                  (progn
                    (copy-partial-batch table-name columns batch (- count half) (+ start half))
                    0)
                (postgresql-retryable (c)
                  (pomo:execute "ROLLBACK")
                  (log-message :error "PostgreSQL [~s] ~a" table-name c)
                  (bisect-batch table-name table columns batch (+ start half) (- count half)))))
        errors)))

;;;
;;; The main retry batch function.
;;;
(defun retry-batch (table columns batch condition
                    &optional (current-batch-pos 0)
                    &aux (nb-errors 0))
  "Batch is a list of rows containing at least one bad row, the first such
   row is known to be located at FIRST-ERROR index in the BATCH array."

  (log-message :info "Entering error recovery.")

  ;; Not all COPY errors include a line number in the CONTEXT field.
  ;; Foreign key violations are a common example: PostgreSQL raises the
  ;; constraint error via a trigger and does not annotate it with the
  ;; "COPY tablename, line N" context that we rely on for efficient recovery.
  ;;
  ;; In that case use a binary search: try the first half of the batch; if it
  ;; succeeds commit it and try the second half; if it fails recurse into the
  ;; failing half.  This finds every bad row in O(k · log N) COPY round-trips
  ;; where k is the number of bad rows, instead of the previous O(N).
  ;;
  (unless (parse-copy-error-context (database-error-context condition))
    (incf nb-errors
          (bisect-batch (format-table-name table) table columns
                        batch 0 (batch-count batch)))
    (log-message :info "Recovery found ~d error~:p in ~d row~:p"
                 nb-errors (batch-count batch))
    (return-from retry-batch nb-errors))

  ;; now deal with the COPY error case where we have a line number and have
  ;; the opportunity to be smart about it.
  (loop
     :with table-name := (format-table-name table)
     :with next-error := (parse-copy-error-context
                          (database-error-context condition))

     :while (< current-batch-pos (batch-count batch))

     :do
     (progn                             ; indenting helper
       (log-message :debug "pos: ~s ; err: ~a" current-batch-pos next-error)
       (when (= current-batch-pos next-error)
         (log-message :info "error recovery at ~d/~d, processing bad row"
                      current-batch-pos (batch-count batch))
         (process-bad-row table
                          condition
                          (aref (batch-data batch) current-batch-pos))
         (incf current-batch-pos)
         (incf nb-errors))

       (let* ((current-batch-rows
               (next-batch-rows (batch-count batch) current-batch-pos next-error)))
         (when (< 0 current-batch-rows)
           (if (< current-batch-pos next-error)
               (log-message :info
                            "error recovery at ~d/~d, next error at ~d, ~
                             loading ~d row~:p"
                            current-batch-pos
                            (batch-count batch)
                            next-error
                            current-batch-rows)
               (log-message :info
                            "error recovery at ~d/~d, trying ~d row~:p"
                            current-batch-pos
                            (batch-count batch)
                            current-batch-rows))

           (handler-case
               (incf current-batch-pos
                     (copy-partial-batch table-name
                                         columns
                                         batch
                                         current-batch-rows
                                         current-batch-pos))

             ;; the batch didn't make it, prepare error handling for next turn
             (postgresql-retryable (next-error-in-batch)
               (pomo:execute "ROLLBACK")
               (log-message :error "PostgreSQL [~s] ~a"
                            table-name
                            next-error-in-batch)
               (let ((next-error-relative
                      (parse-copy-error-context
                       (database-error-context next-error-in-batch))))

                 (if next-error-relative
                     (setf condition  next-error-in-batch
                           next-error (+ current-batch-pos next-error-relative))
                     ;; No COPY line-number context (e.g. FK violation following
                     ;; a format error): fall back to binary search for the
                     ;; remaining rows and exit.
                     (progn
                       (incf nb-errors
                             (bisect-batch table-name table columns
                                           batch current-batch-pos
                                           (- (batch-count batch) current-batch-pos)))
                       (return-from retry-batch nb-errors))))))))))

  (log-message :info "Recovery found ~d errors in ~d row~:p"
               nb-errors (batch-count batch))

  ;; Return how many rows where erroneous, for statistics purposes
  nb-errors)

(defun copy-partial-batch (table-name columns
                           batch current-batch-rows current-batch-pos)
  "Copy some rows of the batch, not all of them."
  (pomo:execute "BEGIN;")
  (let ((stream
         (cl-postgres:open-db-writer pomo:*database* table-name columns)))

    (unwind-protect
         (loop :repeat current-batch-rows
            :for pos :from current-batch-pos
            :do (db-write-row stream (aref (batch-data batch) pos)))

      ;; close-db-writer is the one signaling cl-postgres-errors
      (progn
        (cl-postgres:close-db-writer stream)
        (pomo:execute "COMMIT;")))

    ;; return how many rows we loaded, which is current-batch-rows
    current-batch-rows))

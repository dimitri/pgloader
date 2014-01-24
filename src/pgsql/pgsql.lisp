;;;
;;; The PostgreSQL COPY TO implementation, with batches and retries.
;;;
(in-package :pgloader.pgsql)

;;;
;;; Format row to PostgreSQL COPY format, the TEXT variant.
;;;
;;; That function or something equivalent is provided by default in
;;; cl-postgres, but we want to avoid having to compute its result more than
;;; once in case of a rejected batch. Also, we're using vectors as input to
;;; minimize data copying in certain cases, and we want to avoid a coerce
;;; call here.
;;;
(defun format-vector-row (stream row
                          &key (transforms (loop for c across row collect nil)))
  "Add a ROW in the STREAM, formating ROW in PostgreSQL COPY TEXT format.

See http://www.postgresql.org/docs/9.2/static/sql-copy.html#AEN66609 for
details about the format, and format specs."
  (declare (type simple-array row))
  (let* (*print-circle* *print-pretty*)
    (loop
       with nbcols = (length row)
       for col across row
       for i from 1
       for more? = (< i nbcols)
       for fn in transforms
       for preprocessed-col = (if fn (funcall fn col) col)
       do
         (if (or (null preprocessed-col)
                 ;; still accept postmodern :NULL in "preprocessed" data
                 (eq :NULL preprocessed-col))
             (progn
               ;; NULL is expected as \N, two chars
               (write-char #\\ stream) (write-char #\N stream))
             (loop
                ;; From PostgreSQL docs:
                ;;
                ;; In particular, the following characters must be preceded
                ;; by a backslash if they appear as part of a column value:
                ;; backslash itself, newline, carriage return, and the
                ;; current delimiter character.
                for byte across (cl-postgres-trivial-utf-8:string-to-utf-8-bytes preprocessed-col)
                do (case (code-char byte)
                     (#\\         (progn (write-char #\\ stream)
                                         (write-char #\\ stream)))
                     (#\Space     (write-char #\Space stream))
                     (#\Newline   (progn (write-char #\\ stream)
                                         (write-char #\n stream)))
                     (#\Return    (progn (write-char #\\ stream)
                                         (write-char #\r stream)))
                     (#\Tab       (progn (write-char #\\ stream)
                                         (write-char #\t stream)))
                     (#\Backspace (progn (write-char #\\ stream)
                                         (write-char #\b stream)))
                     (#\Page      (progn (write-char #\\ stream)
                                         (write-char #\f stream)))
                     (t           (if (< 32 byte 127)
                                      (write-char (code-char byte) stream)
                                      (princ (format nil "\\~o" byte) stream))))))
       when more? do (write-char #\Tab stream)
       finally       (write-char #\Newline stream))))

;;;
;;; Pop data from a lparallel.queue queue instance, reformat it assuming
;;; data in there are from cl-mysql, and copy it to a PostgreSQL table.
;;;
(defun copy-batch (table-name columns transforms dataq
                    &key (db pomo:*database*))
  "Copy a batch of data from DATAQ into TABLE-NAME."
  (let ((batch  (make-array *copy-batch-rows* :element-type 'list))
        (count  0))
    (handler-case
        (with-pgsql-transaction (:dbname dbname :database db)
          ;; We need to keep a copy of the rows we send through the COPY
          ;; protocol to PostgreSQL to be able to process them again in case
          ;; of a data error being signaled, that's the BATCH here.
          (let ((copier (cl-postgres:open-db-writer db table-name columns)))
            (unwind-protect
                 (loop for i below *copy-batch-rows*
                    for row = (lq:pop-queue dataq)
                    until (eq row :end-of-data)
                    for data = (with-output-to-string (s)
                                 (format-vector-row s row :transforms transforms))
                    do (progn
                         (incf count)
                         (setf (aref batch i) data)
                         (cl-postgres:db-write-row copier nil data))
                    finally (return count))
              (cl-postgres:close-db-writer copier))))

      ;; If PostgreSQL signals a data error, process the batch by isolating
      ;; erroneous data away and retrying the rest.
      ((or
        CL-POSTGRES-ERROR:INTEGRITY-VIOLATION
        CL-POSTGRES-ERROR:DATA-EXCEPTION) (condition)
        (retry-batch table-name columns batch count condition)))))

;;;
;;; Copy data from queue, a batch at a time.
;;;
(defun copy-from-queue (dbname table-name dataq
			&key
			  columns
			  (truncate t)
			  ((:state *state*) *state*)
			  transforms)
  "Fetch data from the QUEUE until we see :end-of-data. Update *state*"
  (when truncate
    (log-message :notice "TRUNCATE ~a;" table-name)
    (truncate-table dbname table-name))

  (log-message :debug "pgsql:copy-from-queue: ~a ~a" table-name columns)

  (with-pgsql-connection (dbname)
    (loop
       for rows = (copy-batch table-name columns transforms dataq)
       do (progn
            (when (< 0 rows)
              (log-message :debug "copy-batch ~a ~d row~:p" table-name rows))
            (pgstate-incf *state* table-name :rows rows))
       until (lq:queue-empty-p dataq))))

;;;
;;; When a batch has been refused by PostgreSQL with a data-exception, that
;;; means it contains non-conforming data. Log the error message in a log
;;; file and the erroneous data in a rejected data file for further
;;; processing.
;;;
(defun process-bad-row (table-name condition row)
  "Add the row to the reject file, in PostgreSQL COPY TEXT format"
  ;; first, update the stats.
  (pgstate-incf *state* table-name :errs 1)

  ;; now, the bad row processing
  (let* ((table (pgstate-get-table *state* table-name))
	 (data  (pgtable-reject-data table))
	 (logs  (pgtable-reject-logs table)))

    ;; first log the rejected data
    (with-open-file (reject-data-file data
				      :direction :output
				      :if-exists :append
				      :if-does-not-exist :create
				      :external-format :utf-8)
      ;; the row has already been processed when we get here
      (write-string row reject-data-file))

    ;; now log the condition signaled to reject the data
    (with-open-file (reject-logs-file logs
				      :direction :output
				      :if-exists :append
				      :if-does-not-exist :create
				      :external-format :utf-8)
      ;; the row has already been processed when we get here
      (format reject-logs-file "~a~%" condition))))

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
(defun parse-copy-error-context (context)
  "Given a COPY command CONTEXT error message, return the batch position
   where the error comes from."
  (cl-ppcre:register-groups-bind ((#'parse-integer n))
      ("line (\\d+)" context :sharedp t)
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
              CL-POSTGRES-ERROR:INTEGRITY-VIOLATION
              CL-POSTGRES-ERROR:DATA-EXCEPTION) (next-error-in-batch)

              (setf condition next-error-in-batch

                    next-error
                    (+ current-batch-pos
                       (parse-copy-error-context
                        (cl-postgres::database-error-context condition))))))))))

  (log-message :info "Recovery found ~d errors in ~d row~:p" nb-errors batch-rows)

  ;; Return how many rows we did load, for statistics purposes
  (- batch-rows nb-errors))

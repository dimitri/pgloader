;;;
;;; The PostgreSQL COPY TO implementation, with batches and retries.
;;;
(in-package :pgloader.pgsql)

;;;
;;; PostgreSQL formating tools
;;;
(declaim (inline apply-transform-function apply-transforms))

(defun apply-transform-function (fn col)
  "Apply the tranformation function FN to the value COL."
  (if fn (funcall fn col) col))

(defun apply-transforms (row &key transforms)
  "Reformat row as given by MySQL in a format compatible with cl-postgres"
  (loop
     for col in row
     for fn in transforms
     for transformed-col = (apply-transform-function fn col)
     ;; force nil values to being cl-postgres :null special value
     collect (if (null transformed-col) :null transformed-col)))

;;;
;;; Pop data from a lparallel.queue queue instance, reformat it assuming
;;; data in there are from cl-mysql, and copy it to a PostgreSQL table.
;;;
;;; First prepare the streaming function that batches the rows so that we
;;; are able to process them in case of errors.
;;;
(declaim (type (or null simple-array) *batch*))
(declaim (type fixnum *batch-rows*))

(defvar *batch* nil "Current batch of rows being processed.")
(defvar *batch-rows* 0 "How many rows are to be found in current *batch*.")

(defun make-copy-and-batch-fn (stream &key transforms)
  "Returns a function of one argument, ROW.

   When called, the function returned reformats the row, adds it into the
   PostgreSQL COPY STREAM, and push it to BATCH (a list). When batch's size
   is up to *copy-batch-size*, throw the 'next-batch tag with its current
   size."
  (lambda (row)
    (let ((reformated-row (apply-transforms row :transforms transforms)))
      (declare (type simple-array *batch*))

      ;; maintain the current batch
      (setf (aref *batch* *batch-rows*) reformated-row)
      (incf *batch-rows*)

      ;; send the reformated row in the PostgreSQL COPY stream
      (cl-postgres:db-write-row stream reformated-row)

      ;; return control in between batches
      (when (= *batch-rows* *copy-batch-rows*)
	(throw 'next-batch (cons :continue *batch-rows*))))))

;;; The idea is to stream the queue content directly into the
;;; PostgreSQL COPY protocol stream, but COMMIT every
;;; *copy-batch-size* rows.
;;;
;;; That allows to have to recover from a buffer of data only rather
;;; than restart from scratch each time we have to find which row
;;; contains erroneous data. BATCH is that buffer.
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

  ;; Prepare an array we're going to reuse for all batches in the queue.
  (setf *batch* (make-array *copy-batch-rows* :element-type 'list))

  (with-pgsql-connection (dbname)
    (loop
       for retval =
	 (let* ((*batch-rows* 0)
		(*batch-size* 0))
	   (handler-case
	       (with-pgsql-transaction (dbname :database pomo:*database*)
		 (let* ((copier (cl-postgres:open-db-writer pomo:*database*
							    table-name
							    columns)))
		   (log-message :debug "pgsql:copy-from-queue: new batch")
		   (unwind-protect
			(let ((process-row-fn
			       (make-copy-and-batch-fn copier
                                                       :transforms transforms)))
			  (catch 'next-batch
			    (pgloader.queue:map-pop-queue dataq process-row-fn)))

		     (log-message :debug "pgsql:copy-from-queue: batch done")
		     (cl-postgres:close-db-writer copier))))

	     ;; in case of data-exception, split the batch and try again
	     ((or
	       CL-POSTGRES-ERROR:UNIQUE-VIOLATION
	       CL-POSTGRES-ERROR:DATA-EXCEPTION) (e)
	       (declare (ignore e))	; already logged
	      (retry-batch dbname
			   table-name
			   *batch*
			   *batch-rows*
			   :columns columns
			   :transforms transforms))))

       ;; fetch how many rows we just pushed through, update stats
       for rows = (if (consp retval) (cdr retval) retval)
       for cont = (and (consp retval) (eq (car retval) :continue))
       do
	 (pgstate-incf *state* table-name :rows rows)
       while cont)))

;;;
;;; When a batch has been refused by PostgreSQL with a data-exception, that
;;; means it contains non-conforming data. It could be only one row in the
;;; middle of the *copy-batch-size* rows.
;;;
;;; The general principle to filter out the bad row(s) is to split the batch
;;; in smaller ones, and try to COPY all of the smaller ones again,
;;; recursively. When the batch is containing only one row, we know that one
;;; is non conforming to PostgreSQL expectations (usually, data type input
;;; does not match, e.g. text is not proper utf-8).
;;;
;;; As we often need to split out a single bad row out of a full batch, we
;;; don't do the classical dichotomy but rather split the batch directly in
;;; lots of smaller ones.
;;;
;;;   split 1000 rows in 10 batches of 100 rows
;;;   split  352 rows in 3 batches of 100 rows + 1 batch of 52 rows
;;;
(defun process-bad-row (table-name condition row &key transforms)
  "Add the row to the reject file, in PostgreSQL COPY TEXT format"
  ;; first, update the stats.
  (pgstate-incf *state* table-name :errs 1 :rows -1)

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
      (format-row reject-data-file row :transforms transforms))

    ;; now log the condition signaled to reject the data
    (with-open-file (reject-logs-file logs
				      :direction :output
				      :if-exists :append
				      :if-does-not-exist :create
				      :external-format :utf-8)
      ;; the row has already been processed when we get here
      (format reject-logs-file "~a~%" condition))))

;;;
;;; Compute the next batch size in rows, must be smaller than the previous
;;; one or just one row to ensure the retry-batch recursion is not infinite.
;;;
(defun smaller-batch-rows (batch-rows processed-rows)
  "How many rows should we process in next iteration?"
  (let ((remaining-rows (- batch-rows processed-rows)))

    (if (< remaining-rows *copy-batch-split*)
	1
	(min remaining-rows
	     (floor (/ batch-rows *copy-batch-split*))))))

;;;
;;; The recursive retry batch function.
;;;
(defun retry-batch (dbname table-name batch batch-rows
                    &key (current-batch-pos 0) columns transforms)
  "Batch is a list of rows containing at least one bad row. Find it."
  (log-message :debug "pgsql:retry-batch: splitting current batch [~d rows]" batch-rows)
  (let* ((processed-rows 0))
    (loop
       while (< processed-rows batch-rows)
       do
	 (let* ((current-batch-rows
                 (smaller-batch-rows batch-rows processed-rows)))
	  (handler-case
	      (with-pgsql-transaction (dbname :database pomo:*database*)
		(let* ((stream
                        (cl-postgres:open-db-writer pomo:*database*
                                                    table-name columns)))

		  (log-message :debug "pgsql:retry-batch: current-batch-rows = ~d"
			       current-batch-rows)

		  (unwind-protect
                       (loop repeat current-batch-rows
                          for pos from current-batch-pos
                          do (cl-postgres:db-write-row stream (aref *batch* pos))
                          finally
                            (incf current-batch-pos current-batch-rows)
                            (incf processed-rows current-batch-rows))

		    (cl-postgres:close-db-writer stream))))

	    ;; the batch didn't make it, recurse
	    ((or
	      CL-POSTGRES-ERROR:UNIQUE-VIOLATION
	      CL-POSTGRES-ERROR:DATA-EXCEPTION) (condition)
	      ;; process bad data
	      (if (= 1 current-batch-rows)
		  (process-bad-row table-name condition
                                   (aref *batch* current-batch-pos)
				   :transforms transforms)
		  ;; more than one line of bad data: recurse
		  (retry-batch dbname
			       table-name
			       batch
			       current-batch-rows
                               :current-batch-pos current-batch-pos
			       :columns columns
			       :transforms transforms))))))))

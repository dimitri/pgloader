;;;
;;; Tools to handle internal queueing, using lparallel.queue
;;;
(in-package :pgloader.queue)

;;;
;;; The pgloader architectures uses a reader thread and a writer thread. The
;;; reader fills in batches of data from the source of data, and the writer
;;; pushes the data down to PostgreSQL using the COPY protocol.
;;;
;;; The reader thread is always preparing the next batch to send. The reader
;;; thread only works with *reader-batch*.
;;;
;;; As soon as *reader-batch* is ready, it's made available to the writer
;;; thread as *writer-batch*, as soon as the writer is ready for processing
;;; another batch.
;;;
(defstruct batch
  (data  (make-array *copy-batch-rows* :element-type 'simple-string)
         :type (vector simple-string *))
  (count 0 :type fixnum))

(defvar *current-batch* nil)

(defun batch-row (row copy queue)
  "Add ROW to the reader batch. When the batch is full, provide it to the
   writer as the *writer-batch*."
  (when (= (batch-count *current-batch*) *copy-batch-rows*)
    ;; close current batch, prepare next one
    (with-slots (data count) *current-batch*
      (lq:push-queue (list :batch data count) queue))
    (setf *current-batch* (make-batch)))

  ;; Add ROW to the current BATCH.
  ;;
  ;; All the data transformation takes place here, so that we batch fully
  ;; formed COPY TEXT string ready to go in the PostgreSQL stream.
  (let ((copy-string (with-output-to-string (s)
                       (format-vector-row s row (transforms copy)))))
    (with-slots (data count) *current-batch*
      (setf (aref data count) copy-string)
      (incf count))))

(defun map-push-queue (copy queue)
  "Apply MAP-ROWS on the COPY instance and a function of ROW that will push
   the row into the QUEUE. When MAP-ROWS returns, push :end-of-data in the
   queue."
  (setf *current-batch* (make-batch))
  (unwind-protect
       (map-rows copy :process-row-fn (lambda (row) (batch-row row copy queue)))
    (with-slots (data count) *current-batch*
      (when (< 0 count)
        (lq:push-queue (list :batch data count) queue)))

    ;; signal we're done
    (lq:push-queue (list :end-of-data nil nil) queue)))

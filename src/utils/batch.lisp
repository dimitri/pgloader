;;;
;;; Tools to handle internal queueing, using lparallel.queue
;;;
(in-package :pgloader.batch)

;;;
;;; The pgloader architectures uses a reader thread and a writer thread. The
;;; reader fills in batches of data from the source of data, and the writer
;;; pushes the data down to PostgreSQL using the COPY protocol.
;;;
(defstruct batch
  (start (get-internal-real-time) :type fixnum)
  (data  (make-array *copy-batch-rows* :element-type 'simple-string)
         :type (vector simple-string *))
  (count 0 :type fixnum)
  (bytes  0 :type fixnum))

(defvar *current-batch* nil)

(defun finish-current-batch (target queue
                             &optional oversized? (batch *current-batch*))
  (with-slots (start data count) batch
    (when (< 0 count)
      (log-message :debug "finish-current-batch[~a] ~d row~:p in ~6$s"
                   (lp:kernel-worker-index) count
                   (elapsed-time-since start))
      (update-stats :data target
                    :read count
                    :rs (elapsed-time-since start))
      (lq:push-queue (list :batch data count oversized?) queue))))

(declaim (inline oversized?))
(defun oversized? (&optional (batch *current-batch*))
  "Return a generalized boolean that is true only when BATCH is considered
   over-sized when its size in BYTES is compared *copy-batch-size*."
  (and *copy-batch-size*      ; defaults to nil
       (<= *copy-batch-size* (batch-bytes batch))))

(defun batch-row (row target queue)
  "Add ROW to the reader batch. When the batch is full, provide it to the
   writer."
  (let ((oversized? (oversized? *current-batch*)))
    (when (or (= (batch-count *current-batch*) *copy-batch-rows*)
              oversized?)
      ;; close current batch, prepare next one
      (finish-current-batch target queue oversized?)
      (setf *current-batch* (make-batch))))

  (with-slots (data count bytes) *current-batch*
    (setf (aref data count) row)
    (incf count)))


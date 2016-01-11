;;;
;;; Tools to handle internal queueing, using lparallel.queue
;;;
(in-package :pgloader.batch)

;;;
;;; The pgloader architectures uses a reader thread and a writer thread. The
;;; reader fills in batches of data from the source of data, and the writer
;;; pushes the data down to PostgreSQL using the COPY protocol.
;;;
(defstruct (batch
             ;; we use &key as a trick for &aux to see the max-count, think let*
             (:constructor make-batch (&key (max-count (init-batch-max-count))
                                            &aux (data (make-array max-count)))))
  (start     (get-internal-real-time) :type fixnum)
  (data nil                           :type simple-array)
  (count     0                        :type fixnum)
  (max-count 0                        :type fixnum)
  (bytes     0                        :type fixnum))

;;;
;;; The simplest way to avoid all batches being sent at the same time to
;;; PostgreSQL is to make them of different sizes. Here we tweak the batch
;;; size from *copy-batch-rows* to that effect.
;;;
(defun init-batch-max-count (&optional (batch-rows *copy-batch-rows*))
  "Return a number between 0.7 and 1.3 times batch-rows."
  ;; 0.7 < 0.7 + (random 0.6) < 1.3
  (truncate (* batch-rows (+ 0.7 (random 0.6)))))

(defun finish-batch (batch target queue &optional oversized?)
  (with-slots (start data count) batch
    (when (< 0 count)
      (log-message :debug "finish-batch[~a] ~d row~:p in ~6$s"
                   (lp:kernel-worker-index) count
                   (elapsed-time-since start))
      (update-stats :data target
                    :read count
                    :rs (elapsed-time-since start))
      (lq:push-queue (list :batch data count oversized?) queue))))

(defun push-end-of-data-message (queue)
  "Push a fake batch marker to signal end-of-data in QUEUE."
  ;; the message must look like the finish-batch message overall
  (lq:push-queue (list :end-of-data nil nil nil) queue))

(declaim (inline oversized?))
(defun oversized? (batch)
  "Return a generalized boolean that is true only when BATCH is considered
   over-sized when its size in BYTES is compared *copy-batch-size*."
  (and *copy-batch-size*      ; defaults to nil
       (<= *copy-batch-size* (batch-bytes batch))))

(defun batch-row (batch row target queue)
  "Add ROW to the reader batch. When the batch is full, provide it to the
   writer."
  (let ((maybe-new-batch
         (let ((oversized? (oversized? batch)))
           (if (or (= (batch-count batch) (batch-max-count batch))
                   oversized?)
               (progn
                 ;; close current batch, prepare next one
                 (finish-batch batch target queue oversized?)
                 (make-batch))

               ;; return given batch, it's still current
               batch))))

    (with-slots (data count bytes) maybe-new-batch
      (setf (aref data count) row)
      (incf count))

    maybe-new-batch))


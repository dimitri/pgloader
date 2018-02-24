;;;
;;; Tools to handle internal queueing, using lparallel.queue
;;;
(in-package :pgloader.pgcopy)

;;;
;;; The pgloader architectures uses a reader thread and a writer thread. The
;;; reader fills in batches of data from the source of data, and the writer
;;; pushes the data down to PostgreSQL using the COPY protocol.
;;;
(defstruct (batch
             (:constructor
              make-batch (&key
                          (max-count (init-batch-max-count))
                          &aux
                          (data
                           (make-array max-count
                                       :element-type '(simple-array
                                                       (unsigned-byte 8)))))))
  (start     (get-internal-real-time) :type fixnum)
  (data      nil                      :type array)
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

(defun batch-oversized-p (batch)
  "Return a generalized boolean that is true only when BATCH is considered
   over-sized when its size in BYTES is compared *copy-batch-size*."
  (and *copy-batch-size*                ; defaults to nil
       (<= *copy-batch-size* (batch-bytes batch))))

(defun batch-full-p (batch)
  (or (= (batch-count batch) (batch-max-count batch))
      (batch-oversized-p batch)))

(defun push-row (batch row row-bytes)
  (with-slots (data count bytes) batch
    (setf (aref data count) row)
    (incf count)
    (incf bytes row-bytes)))

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


;;;
;;; Integration of batch with COPY row format
;;;
(defun format-row-in-batch (copy nbcols row current-batch)
  "Given a row from the queue, prepare it for the next batch."
  (multiple-value-bind (pg-vector-row bytes)
      (prepare-and-format-row copy nbcols row)
    (when pg-vector-row
      (push-row current-batch pg-vector-row bytes))))

(defun add-row-to-current-batch (table columns copy nbcols batch row
                                 &key send-batch-fn format-row-fn)
  "Add another ROW we just received to CURRENT-BATCH, and prepare a new
   batch if needed. The current-batch (possibly a new one) is returned. When
   the batch is full, the function SEND-BATCH-FN is called with TABLE,
   COLUMNS and the full BATCH as parameters."
  (let ((seconds       0)
        (current-batch batch))
    ;; if current-batch is full, send data to PostgreSQL
    ;; and prepare a new batch
    (when (batch-full-p current-batch)
      (incf seconds (funcall send-batch-fn table columns current-batch))
      (setf current-batch (make-batch))

      ;; give a little help to our friend, now is a good time
      ;; to garbage collect
      #+sbcl
      (let ((garbage-collect-start (get-internal-real-time)))
        (sb-ext:gc :full t)
        (incf seconds (elapsed-time-since garbage-collect-start))))

    ;; also add up the time it takes to format the rows
    (let ((start-time (get-internal-real-time)))
      (multiple-value-bind (pg-vector-row bytes)
          (funcall format-row-fn copy nbcols row)
        (when pg-vector-row
          (push-row current-batch pg-vector-row bytes)))
      (incf seconds (elapsed-time-since start-time)))

    (values current-batch seconds)))

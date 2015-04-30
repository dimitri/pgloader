;;;
;;; Tools to handle internal queueing, using lparallel.queue
;;;
(in-package :pgloader.queue)

;;;
;;; The pgloader architectures uses a reader thread and a writer thread. The
;;; reader fills in batches of data from the source of data, and the writer
;;; pushes the data down to PostgreSQL using the COPY protocol.
;;;
(defstruct batch
  (data  (make-array *copy-batch-rows* :element-type 'simple-string)
         :type (vector simple-string *))
  (count 0 :type fixnum)
  (bytes  0 :type fixnum))

(defvar *current-batch* nil)

(declaim (inline oversized?))
(defun oversized? (&optional (batch *current-batch*))
  "Return a generalized boolean that is true only when BATCH is considered
   over-sized when its size in BYTES is compared *copy-batch-size*."
  (and *copy-batch-size*      ; defaults to nil
       (<= *copy-batch-size* (batch-bytes batch))))

(defun batch-row (row copy queue &optional pre-formatted)
  "Add ROW to the reader batch. When the batch is full, provide it to the
   writer."
  (when (or (eq :data *log-min-messages*)
            (eq :data *client-min-messages*))
    (log-message :data "< ~s" row))
  (let ((oversized? (oversized? *current-batch*)))
    (when (or (= (batch-count *current-batch*) *copy-batch-rows*)
              oversized?)
      ;; close current batch, prepare next one
      (with-slots (data count bytes) *current-batch*
        (lq:push-queue (list :batch data count oversized?) queue))
      (setf *current-batch* (make-batch))))

  ;; Add ROW to the current BATCH.
  ;;
  ;; All the data transformation takes place here, so that we batch fully
  ;; formed COPY TEXT string ready to go in the PostgreSQL stream.
  (handler-case
      (with-slots (data count bytes) *current-batch*
        (let ((copy-string
               (with-output-to-string (s)
                 (let ((c-s-bytes (format-vector-row s row
                                                     (transforms copy)
                                                     pre-formatted)))
                   (when *copy-batch-size* ; running under memory watch
                     (incf bytes c-s-bytes))))))
          (setf (aref data count) copy-string)
          (incf count)))

    (condition (e)
      (log-message :error "~a" e))))

(defun map-push-queue (copy queue &optional pre-formatted)
  "Apply MAP-ROWS on the COPY instance and a function of ROW that will push
   the row into the QUEUE. When MAP-ROWS returns, push :end-of-data in the
   queue."
  (unwind-protect
       (let ((*current-batch* (make-batch)))
         (map-rows copy :process-row-fn (lambda (row)
                                          (batch-row row copy queue
                                                     pre-formatted)))

         ;; we might have the last batch to send over now
         (with-slots (data count) *current-batch*
           (when (< 0 count)
             (log-message :debug "Sending last batch (~d rows)" count)
             (lq:push-queue (list :batch data count nil) queue))))

    ;; signal we're done
    (log-message :debug "End of data.")
    (lq:push-queue (list :end-of-data nil nil nil) queue)))

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
  (count 0 :type fixnum)
  (bytes  0 :type fixnum))

(defvar *current-batch* nil)

(declaim (inline oversized?))
(defun oversized? (&optional (batch *current-batch*))
  "Return a generalized boolean that is true only when BATCH is considered
   over-sized when its size in BYTES is compared *copy-batch-size*."
  (and *copy-batch-size*      ; defaults to nil
       (<= *copy-batch-size* (batch-bytes batch))))

(defun batch-row (row copy queue)
  "Add ROW to the reader batch. When the batch is full, provide it to the
   writer as the *writer-batch*."
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
      (let ((copy-string (with-output-to-string (s)
                           (format-vector-row s row (transforms copy)))))
        (with-slots (data count bytes) *current-batch*
          (setf (aref data count) copy-string)
          (when *copy-batch-size*          ; running under memory watch
            (incf bytes
                  #+sbcl (length
                          (sb-ext:string-to-octets copy-string :external-format :utf-8))
                  #+ccl (ccl:string-size-in-octets copy-string :external-format :utf-8)
                  #- (or sbcl ccl) (length copy-string)))
          (incf count)))

    (condition (e)
      (log-message :error "~a" e))))

(defun map-push-queue (copy queue)
  "Apply MAP-ROWS on the COPY instance and a function of ROW that will push
   the row into the QUEUE. When MAP-ROWS returns, push :end-of-data in the
   queue."
  (unwind-protect
       (let ((*current-batch* (make-batch)))
         (map-rows copy :process-row-fn (lambda (row)
                                          (batch-row row copy queue)))

         ;; we might have the last batch to send over now
         (with-slots (data count) *current-batch*
           (when (< 0 count)
             (lq:push-queue (list :batch data count nil) queue))))

    ;; signal we're done
    (lq:push-queue (list :end-of-data nil nil nil) queue)))

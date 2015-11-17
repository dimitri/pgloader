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
  (start (get-internal-real-time) :type fixnum)
  (data  (make-array *copy-batch-rows* :element-type 'simple-string)
         :type (vector simple-string *))
  (count 0 :type fixnum)
  (bytes  0 :type fixnum))

(defvar *current-batch* nil)

(defun cook-batches (copy raw-queue processed-queue &optional pre-formatted)
  "Cook data from raw-queue into batches sent into processed-queue."
  (let ((*current-batch* (make-batch)))
    (loop :for row := (lq:pop-queue raw-queue)
       :until (eq :end-of-data row)
       :do (batch-row row copy processed-queue pre-formatted))

    ;; finish current-batch
    (finish-current-batch copy processed-queue)

    ;; and before calling it a day, push the end-of-data marker
    (log-message :debug "End of data.")

    ;; we hardcode 2 parallel COPY writers, see copy-from implementation.
    (lq:push-queue (list :end-of-data nil nil nil) processed-queue)
    (lq:push-queue (list :end-of-data nil nil nil) processed-queue)))

(defun finish-current-batch (copy queue
                             &optional oversized? (batch *current-batch*))
  (with-slots (start data count) batch
    (when (< 0 count)
      (update-stats :data (target copy)
                    :read count
                    :rs (elapsed-time-since start))
      (lq:push-queue (list :batch data count oversized?) queue))))

(declaim (inline oversized?))
(defun oversized? (&optional (batch *current-batch*))
  "Return a generalized boolean that is true only when BATCH is considered
   over-sized when its size in BYTES is compared *copy-batch-size*."
  (and *copy-batch-size*      ; defaults to nil
       (<= *copy-batch-size* (batch-bytes batch))))

(defun batch-row (row copy queue &optional pre-formatted)
  "Add ROW to the reader batch. When the batch is full, provide it to the
   writer."
  (let ((oversized? (oversized? *current-batch*)))
    (when (or (= (batch-count *current-batch*) *copy-batch-rows*)
              oversized?)
      ;; close current batch, prepare next one
      (finish-current-batch copy queue oversized?)
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
      (log-message :error "~a" e)
      (update-stats :data (target copy) :errs 1))))


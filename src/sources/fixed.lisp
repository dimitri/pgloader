;;;
;;; Tools to handle fixed width files
;;;

(in-package :pgloader.fixed)

(defclass fixed-connection (md-connection) ())

(defmethod initialize-instance :after ((fixed fixed-connection) &key)
  "Assign the type slot to sqlite."
  (setf (slot-value fixed 'type) "fixed"))

(defclass copy-fixed (md-copy)
  ((encoding    :accessor encoding	  ; file encoding
	        :initarg :encoding)	  ;
   (skip-lines  :accessor skip-lines	  ; CSV headers
	        :initarg :skip-lines	  ;
		:initform 0))
  (:documentation "pgloader Fixed Columns Data Source"))

(defmethod initialize-instance :after ((fixed copy-fixed) &key)
  "Compute the real source definition from the given source parameter, and
   set the transforms function list as needed too."
  (let ((transforms (when (slot-boundp fixed 'transforms)
		      (slot-value fixed 'transforms)))
	(columns
	 (or (slot-value fixed 'columns)
	     (pgloader.pgsql:list-columns (slot-value fixed 'target-db)
					  (slot-value fixed 'target)))))
    (unless transforms
      (setf (slot-value fixed 'transforms) (make-list (length columns))))))

(declaim (inline parse-row))

(defun parse-row (fixed-cols-specs line)
  "Parse a single line of FIXED input file and return a row of columns."
  (loop :with len := (length line)
     :for opts :in fixed-cols-specs
     :collect (destructuring-bind (&key start length &allow-other-keys) opts
                ;; some fixed format files are ragged on the right, meaning
                ;; that we might have missing characters on each line.
                ;; take all that we have and return nil for missing data.
                (let ((end (+ start length)))
                  (when (<= start len)
                    (subseq line start (min len end)))))))

(defmethod process-rows ((fixed copy-fixed) stream process-fn)
  "Process rows from STREAM according to COPY specifications and PROCESS-FN."
  (loop
     :with fun := process-fn
     :with fixed-cols-specs := (mapcar #'cdr (fields fixed))
     :for line := (read-line stream nil nil)
     :counting line :into read
     :while line
     :do (handler-case
             (funcall fun (parse-row fixed-cols-specs line))
           (condition (e)
             (progn
               (log-message :error "~a" e)
               (update-stats :data (target fixed) :errs 1))))))

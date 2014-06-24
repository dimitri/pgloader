;;;
;;; Tools to handle MySQL data fetching
;;;

(in-package :pgloader.fixed)

(defclass copy-fixed (copy)
  ((source-type :accessor source-type	  ; one of :inline, :stdin, :regex
		:initarg :source-type)	  ;  or :filename
   (encoding    :accessor encoding	  ; file encoding
	        :initarg :encoding)	  ;
   (skip-lines  :accessor skip-lines	  ; CSV headers
	        :initarg :skip-lines	  ;
		:initform 0))
  (:documentation "pgloader Fixed Columns Data Source"))

(defmethod initialize-instance :after ((fixed copy-fixed) &key)
  "Compute the real source definition from the given source parameter, and
   set the transforms function list as needed too."
  (let ((source (slot-value fixed 'source)))
    (setf (slot-value fixed 'source-type) (car source))
    (setf (slot-value fixed 'source)      (get-absolute-pathname source)))

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
                (let ((end (+ start length)))
                  (when (<= start len)
                    (subseq line start (min len end)))))))

(defmethod map-rows ((fixed copy-fixed) &key process-row-fn)
  "Load data from a text file in Fixed Columns format.

   Each row is pre-processed then PROCESS-ROW-FN is called with the row as a
   list as its only parameter.

   Returns how many rows where read and processed."
  (let ((filenames   (case (source-type fixed)
                       (:stdin   (list (source fixed)))
		       (:inline  (list (car (source fixed))))
		       (:regex   (source fixed))
		       (t        (list (source fixed))))))
    (loop :for filename :in filenames
       :do
	 (with-open-file-or-stream
	     ;; we just ignore files that don't exist
	     (input filename
		    :direction :input
		    :external-format (encoding fixed)
		    :if-does-not-exist nil)
	   (when input
	     ;; first go to given inline position when filename is :inline
	     (when (eq (source-type fixed) :inline)
	       (file-position input (cdr (source fixed))))

	     ;; ignore as much as skip-lines lines in the file
	     (loop repeat (skip-lines fixed) do (read-line input nil nil))

	     ;; read in the text file, split it into columns, process NULL
	     ;; columns the way postmodern expects them, and call
	     ;; PROCESS-ROW-FN on them
	     (let ((reformat-then-process
		    (reformat-then-process :fields  (fields fixed)
					   :columns (columns fixed)
					   :target  (target fixed)
					   :process-row-fn process-row-fn)))
	       (loop
		  :with fun := (compile nil reformat-then-process)
                  :with fixed-cols-specs := (mapcar #'cdr (fields fixed))
		  :for line := (read-line input nil nil)
		  :counting line :into read
		  :while line
		  :do (handler-case
                          (funcall fun (parse-row fixed-cols-specs line))
                        (condition (e)
                          (progn
                            (log-message :error "~a" e)
                            (pgstate-incf *state* (target fixed) :errs 1)))))))))))

(defmethod copy-to-queue ((fixed copy-fixed) queue)
  "Copy data from given FIXED definition into lparallel.queue DATAQ"
  (pgloader.queue:map-push-queue fixed queue))

(defmethod copy-from ((fixed copy-fixed) &key truncate)
  "Copy data from given FIXED file definition into its PostgreSQL target table."
  (let* ((summary        (null *state*))
	 (*state*        (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel*    (make-kernel 2))
	 (channel        (lp:make-channel))
	 (queue          (lq:make-queue :fixed-capacity *concurrent-batches*))
	 (dbname         (target-db fixed))
	 (table-name     (target fixed)))

    (with-stats-collection (table-name :state *state* :summary summary)
      (log-message :notice "COPY ~a.~a" dbname table-name)
      (lp:submit-task channel #'copy-to-queue fixed queue)

      ;; and start another task to push that data from the queue to PostgreSQL
      (lp:submit-task channel
		      ;; this function update :rows stats
		      #'pgloader.pgsql:copy-from-queue dbname table-name queue
		      ;; we only are interested into the column names here
		      :columns (mapcar #'car (columns fixed))
		      :truncate truncate)

      ;; now wait until both the tasks are over
      (loop for tasks below 2 do (lp:receive-result channel)
	 finally (lp:end-kernel)))))


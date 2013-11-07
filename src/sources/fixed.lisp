;;;
;;; Tools to handle MySQL data fetching
;;;

(in-package :pgloader.fixed)

(defclass copy-fixed (copy)
  ((encoding    :accessor encoding	  ; file encoding
	        :initarg :encoding)	  ;
   (skip-lines  :accessor skip-lines	  ; CSV headers
	        :initarg :skip-lines	  ;
		:initform 0))
  (:documentation "pgloader Fixed Columns Data Source"))

(defmethod initialize-instance :after ((fixed copy-fixed) &key)
  "Compute the real source definition from the given source parameter, and
   set the transforms function list as needed too."
  (let ((source (slot-value fixed 'source)))
    (setf (slot-value fixed 'source) (get-absolute-pathname source)))

  (let ((transforms (when (slot-boundp fixed 'transforms)
		      (slot-value fixed 'transforms)))
	(columns
	 (or (slot-value fixed 'columns)
	     (pgloader.pgsql:list-columns (slot-value fixed 'target-db)
					  (slot-value fixed 'target)))))
    (unless transforms
      (setf (slot-value fixed 'transforms)
	    (loop for c in columns collect nil)))))

(declaim (inline parse-row))

(defmethod parse-row ((fixed copy-fixed) line)
  "Parse a single line of FIXED input file and return a row of columns."
  (loop for (name . opts) in (fields fixed)
     collect (destructuring-bind (&key start length &allow-other-keys) opts
	       (subseq line start (+ start length)))))

(defmethod map-rows ((fixed copy-fixed) &key process-row-fn)
  "Load data from a text file in Fixed Columns format.

   Each row is pre-processed then PROCESS-ROW-FN is called with the row as a
   list as its only parameter.

   Returns how many rows where read and processed."
  (let* ((filespec   (source fixed))
	 (filename   (if (consp filespec) (car filespec) filespec)))
   (with-open-file
       ;; we just ignore files that don't exist
       (input filename
	      :direction :input
	      :external-format (encoding fixed)
	      :if-does-not-exist nil)
     (when input
       ;; first go to given inline position when filename is a consp
       (when (consp filespec)
	 (loop repeat (cdr filespec) do (read-char input)))

       ;; ignore as much as skip-lines lines in the file
       (loop repeat (skip-lines fixed) do (read-line input nil nil))

       ;; read in the text file, split it into columns, process NULL columns
       ;; the way postmodern expects them, and call PROCESS-ROW-FN on them
       (let* ((read 0)
	      (projection (project-fields :fields  (fields fixed)
					  :columns (columns fixed)))
	      (reformat-then-process
	       (lambda (row)
		 (let ((projected-row
			(handler-case
			    (funcall projection row)
			  (condition (e)
			    (pgstate-incf *state* (target fixed) :errs 1)
			    (log-message :error
					 "Could not read line ~d: ~a" read e)))))
		   (when projected-row
		     (funcall process-row-fn projected-row))))))
	 (loop
	    with fun = (compile nil reformat-then-process)
	    for line = (read-line input nil nil)
	    counting line into read
	    while line
	    do (funcall fun (parse-row fixed line))
	    finally (return read)))))))

(defmethod copy-to-queue ((fixed copy-fixed) dataq)
  "Copy data from given FIXED definition into lparallel.queue DATAQ"
  (let ((read (pgloader.queue:map-push-queue dataq #'map-rows fixed)))
    (pgstate-incf *state* (target fixed) :read read)))

(defmethod copy-from ((fixed copy-fixed) &key truncate)
  "Copy data from given FIXED file definition into its PostgreSQL target table."
  (let* ((summary        (null *state*))
	 (*state*        (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel*    (make-kernel 2))
	 (channel        (lp:make-channel))
	 (dataq          (lq:make-queue :fixed-capacity 4096))
	 (dbname         (target-db fixed))
	 (table-name     (target fixed)))

    (with-stats-collection (dbname table-name :state *state* :summary summary)
      (log-message :notice "COPY ~a.~a" dbname table-name)
      (lp:submit-task channel #'copy-to-queue fixed dataq)

      ;; and start another task to push that data from the queue to PostgreSQL
      (lp:submit-task channel
		      ;; this function update :rows stats
		      #'pgloader.pgsql:copy-from-queue dbname table-name dataq
		      ;; we only are interested into the column names here
		      :columns (let ((cols (columns fixed)))
				 (when cols (mapcar #'car cols)))
		      :truncate truncate
		      :transforms (transforms fixed))

      ;; now wait until both the tasks are over
      (loop for tasks below 2 do (lp:receive-result channel)
	 finally (lp:end-kernel)))))


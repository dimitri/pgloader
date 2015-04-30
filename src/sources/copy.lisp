;;;
;;; Read a file format in PostgreSQL COPY TEXT format.
;;;
(in-package :pgloader.copy)

(defclass copy-connection (csv-connection) ())

(defmethod initialize-instance :after ((csvconn copy-connection) &key)
  "Assign the type slot to sqlite."
  (setf (slot-value csvconn 'type) "copy"))

(defclass copy-copy (copy)
  ((source-type :accessor source-type	  ; one of :inline, :stdin, :regex
		:initarg :source-type)	  ;  or :filename
   (encoding    :accessor encoding	  ; file encoding
	        :initarg :encoding)	  ;
   (skip-lines  :accessor skip-lines	  ; we might want to skip COPY lines
	        :initarg :skip-lines	  ;
		:initform 0))
  (:documentation "pgloader COPY Data Source"))

(defmethod initialize-instance :after ((copy copy-copy) &key)
  "Compute the real source definition from the given source parameter, and
   set the transforms function list as needed too."
  (let ((source (csv-specs (slot-value copy 'source))))
    (setf (slot-value copy 'source-type) (car source))
    (setf (slot-value copy 'source)      (get-absolute-pathname source)))

  (let ((transforms (when (slot-boundp copy 'transforms)
		      (slot-value copy 'transforms)))
	(columns
         (or (slot-value copy 'columns)
             (pgloader.pgsql:list-columns (slot-value copy 'target-db)
                                          (slot-value copy 'target)))))
    (unless transforms
      (setf (slot-value copy 'transforms) (make-list (length columns))))))

(declaim (inline parse-row))

(defun parse-row (line)
  "Parse a single line of COPY input file and return a row of columns."
  (mapcar (lambda (x)
            ;; we want Postmodern compliant NULLs
            (if (string= "\\N" x) :null x))
          ;; splitting is easy, it's always on #\Tab
          ;; see format-row-for-copy for details
          (sq:split-sequence #\Tab line)))

(defmethod map-rows ((copy copy-copy) &key process-row-fn)
  "Load data from a text file in Copy Columns format.

   Each row is pre-processed then PROCESS-ROW-FN is called with the row as a
   list as its only parameter.

   Returns how many rows were read and processed."
  (let ((filenames   (case (source-type copy)
                       (:stdin   (list (source copy)))
		       (:inline  (list (car (source copy))))
		       (:regex   (source copy))
		       (t        (list (source copy))))))
    (loop :for filename :in filenames
       :do
	 (with-open-file-or-stream
	     ;; we just ignore files that don't exist
	     (input filename
		    :direction :input
		    :external-format (encoding copy)
		    :if-does-not-exist nil)
	   (when input
	     ;; first go to given inline position when filename is :inline
	     (when (eq (source-type copy) :inline)
	       (file-position input (cdr (source copy))))

	     ;; ignore as much as skip-lines lines in the file
	     (loop repeat (skip-lines copy) do (read-line input nil nil))

	     ;; read in the text file, split it into columns, process NULL
	     ;; columns the way postmodern expects them, and call
	     ;; PROCESS-ROW-FN on them
	     (let ((reformat-then-process
		    (reformat-then-process :fields  (fields copy)
					   :columns (columns copy)
					   :target  (target copy)
					   :process-row-fn process-row-fn)))
	       (loop
		  :with fun := reformat-then-process
		  :for line := (read-line input nil nil)
		  :counting line :into read
		  :while line
		  :do (handler-case
                          (funcall fun (parse-row line))
                        (condition (e)
                          (progn
                            (log-message :error "~a" e)
                            (pgstate-incf *state* (target copy) :errs 1)))))))))))

(defmethod copy-to-queue ((copy copy-copy) queue)
  "Copy data from given COPY definition into lparallel.queue DATAQ"
  (pgloader.queue:map-push-queue copy queue 'pre-formatted))

(defmethod copy-from ((copy copy-copy) &key truncate disable-triggers)
  "Copy data from given COPY file definition into its PostgreSQL target table."
  (let* ((summary        (null *state*))
	 (*state*        (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel*    (make-kernel 2))
	 (channel        (lp:make-channel))
	 (queue          (lq:make-queue :fixed-capacity *concurrent-batches*)))

    (with-stats-collection ((target copy)
                            :dbname (db-name (target-db copy))
                            :state *state*
                            :summary summary)
      (lp:task-handler-bind ((error #'lp:invoke-transfer-error))
        (log-message :notice "COPY ~a" (target copy))
        (lp:submit-task channel #'copy-to-queue copy queue)

        ;; and start another task to push that data from the queue to PostgreSQL
        (lp:submit-task channel
                        ;; this function update :rows stats
                        #'pgloader.pgsql:copy-from-queue
                        (target-db copy) (target copy) queue
                        ;; we only are interested into the column names here
                        :columns (mapcar (lambda (col)
                                           ;; always double quote column names
                                           (format nil "~s" (car col)))
                                         (columns copy))
                        :truncate truncate
                        :disable-triggers disable-triggers)

        ;; now wait until both the tasks are over
        (loop for tasks below 2 do (lp:receive-result channel)
           finally (lp:end-kernel))))))


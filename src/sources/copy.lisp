;;;
;;; Read a file format in PostgreSQL COPY TEXT format.
;;;
(in-package :pgloader.copy)

(defclass copy-connection (md-connection) ())

(defmethod initialize-instance :after ((copy copy-connection) &key)
  "Assign the type slot to sqlite."
  (setf (slot-value copy 'type) "copy"))

(defclass copy-copy (copy)
  ((encoding    :accessor encoding	  ; file encoding
	        :initarg :encoding)	  ;
   (skip-lines  :accessor skip-lines	  ; we might want to skip COPY lines
	        :initarg :skip-lines	  ;
		:initform 0)              ;
   (delimiter   :accessor delimiter       ; see COPY options for TEXT
                :initarg :delimiter       ; in PostgreSQL docs
                :initform #\Tab)
   (null-as     :accessor null-as
                :initarg :null-as
                :initform "\\N"))
  (:documentation "pgloader COPY Data Source"))

(defmethod initialize-instance :after ((copy copy-copy) &key)
  "Compute the real source definition from the given source parameter, and
   set the transforms function list as needed too."
  (let ((transforms (when (slot-boundp copy 'transforms)
		      (slot-value copy 'transforms)))
	(columns
         (or (slot-value copy 'columns)
             (pgloader.pgsql:list-columns (slot-value copy 'target-db)
                                          (slot-value copy 'target)))))
    (unless transforms
      (setf (slot-value copy 'transforms) (make-list (length columns))))))

(declaim (inline parse-row))

(defun parse-row (line &key (delimiter #\Tab) (null-as "\\N"))
  "Parse a single line of COPY input file and return a row of columns."
  (mapcar (lambda (x)
            ;; we want Postmodern compliant NULLs
            (cond ((string= null-as x) :null)

                  ;; and we want to avoid injecting default NULL
                  ;; representation down to PostgreSQL when null-as isn't
                  ;; the default
                  ((and (string/= null-as "\\N") (string= x "\\N"))
                   ;; escape the backslash
                   "\\\\N")

                  ;; default case, just use the value we've just read
                  (t x)))
          ;; splitting is easy, it's always on #\Tab
          ;; see format-row-for-copy for details
          (sq:split-sequence delimiter line)))

(defmethod map-rows ((copy copy-copy) &key process-row-fn)
  "Load data from a text file in Copy Columns format.

   Each row is pre-processed then PROCESS-ROW-FN is called with the row as a
   list as its only parameter.

   Returns how many rows were read and processed."
  (with-connection (cnx (source copy))
    (loop :for input := (open-next-stream cnx
                                          :direction :input
                                          :external-format (encoding copy)
                                          :if-does-not-exist nil)
       :while input
       :do (progn
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
                          (funcall fun (parse-row line
                                                  :delimiter (delimiter copy)
                                                  :null-as   (null-as copy)))
                        (condition (e)
                          (progn
                            (log-message :error "~a" e)
                            (update-stats :data (target copy) :errs 1))))))))))

(defmethod copy-to-queue ((copy copy-copy) queue)
  "Copy data from given COPY definition into lparallel.queue DATAQ"
  (pgloader.queue:map-push-queue copy queue 'pre-formatted))

(defmethod copy-from ((copy copy-copy)
                      &key
                        truncate
                        disable-triggers
                        drop-indexes)
  "Copy data from given COPY file definition into its PostgreSQL target table."
  (let* ((lp:*kernel*    (make-kernel 2))
	 (channel        (lp:make-channel))
	 (queue          (lq:make-queue :fixed-capacity *concurrent-batches*))
         (indexes        (maybe-drop-indexes (target-db copy)
                                             (target copy)
                                             :drop-indexes drop-indexes)))

    (with-stats-collection ((target copy) :dbname (db-name (target-db copy)))
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
           finally (lp:end-kernel))))

    ;; re-create the indexes
    (create-indexes-again (target-db copy) indexes
                          :drop-indexes drop-indexes)))


;;;
;;; Tools to handle the DBF file format
;;;

(in-package :pgloader.db3)

;;;
;;; Integration with pgloader
;;;
(defclass copy-db3 (copy)
  ((encoding    :accessor encoding	  ; file encoding
	        :initarg :encoding))
  (:documentation "pgloader DBF Data Source"))

(defmethod initialize-instance :after ((db3 copy-db3) &key)
  "Add a default value for transforms in case it's not been provided."
  (setf (slot-value db3 'source) (pathname-name (fd-path (source-db db3))))

  (with-connection (conn (source-db db3))
    (unless (and (slot-boundp db3 'columns) (slot-value db3 'columns))
      (setf (slot-value db3 'columns)
            (list-all-columns (fd-db3 conn)
                              (or (target db3) (source db3)))))

    (let ((transforms (when (slot-boundp db3 'transforms)
                        (slot-value db3 'transforms))))
      (unless transforms
        (setf (slot-value db3 'transforms)
              (list-transforms (fd-db3 conn)))))))

(defmethod map-rows ((copy-db3 copy-db3) &key process-row-fn)
  "Extract DB3 data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (with-connection (conn (source-db copy-db3))
    (let ((stream (conn-handle (source-db copy-db3)))
          (db3    (fd-db3 (source-db copy-db3)))
          (db3:*external-format* (encoding copy-db3)))
      (loop
         :with count := (db3:record-count db3)
         :repeat count
         :for row-array := (db3:load-record db3 stream)
         :do (funcall process-row-fn row-array)
         :finally (return count)))))

(defmethod copy-to ((db3 copy-db3) pgsql-copy-filename)
  "Extract data from DB3 file into a PotgreSQL COPY TEXT formated file"
  (with-open-file (text-file pgsql-copy-filename
			     :direction :output
			     :if-exists :supersede
			     :external-format :utf-8)
    (let ((transforms (list-transforms (source db3))))
      (map-rows db3
		:process-row-fn
		(lambda (row)
		  (format-vector-row text-file row transforms))))))

(defmethod copy-to-queue ((db3 copy-db3) queue)
  "Copy data from DB3 file FILENAME into queue DATAQ"
  (let ((read (pgloader.queue:map-push-queue db3 queue)))
    (pgstate-incf *state* (target db3) :read read)))

(defmethod copy-from ((db3 copy-db3)
                      &key (kernel nil k-s-p) truncate disable-triggers)
  (let* ((summary        (null *state*))
         (*state*        (or *state* (pgloader.utils:make-pgstate)))
         (lp:*kernel*    (or kernel (make-kernel 2)))
         (channel        (lp:make-channel))
         (queue          (lq:make-queue :fixed-capacity *concurrent-batches*)))

    (with-stats-collection ((target db3)
                            :dbname (db-name (target-db db3))
                            :state *state*
                            :summary summary)
      (lp:task-handler-bind ((error #'lp:invoke-transfer-error))
        (log-message :notice "COPY \"~a\" from '~a'" (target db3) (source db3))
        (lp:submit-task channel #'copy-to-queue db3 queue)

        ;; and start another task to push that data from the queue to PostgreSQL
        (lp:submit-task channel
                        #'pgloader.pgsql:copy-from-queue
                        (target-db db3) (target db3) queue
                        :truncate truncate
                        :disable-triggers disable-triggers)

        ;; now wait until both the tasks are over, and kill the kernel
        (loop for tasks below 2 do (lp:receive-result channel)
           finally
             (log-message :info "COPY \"~a\" done." (target db3))
             (unless k-s-p (lp:end-kernel)))))))

(defmethod copy-database ((db3 copy-db3)
                          &key
                            table-name
                            state-before
			    data-only
			    schema-only
                            (truncate         t)
                            (disable-triggers nil)
                            (create-tables    t)
			    (include-drop     t)
			    (create-indexes   t)
			    (reset-sequences  t))
  "Open the DB3 and stream its content to a PostgreSQL database."
  (declare (ignore create-indexes reset-sequences))
  (let* ((summary     (null *state*))
	 (*state*     (or *state* (make-pgstate)))
	 (table-name  (or table-name
			  (target db3)
			  (source db3))))

    ;; fix the table-name in the db3 object
    (setf (target db3) table-name)

    (handler-case
        (when (and (or create-tables schema-only) (not data-only))
          (with-stats-collection ("create, truncate"
                                  :state state-before
                                  :summary summary)
            (with-pgsql-transaction (:pgconn (target-db db3))
              (when create-tables
                (log-message :notice "Create table \"~a\"" table-name)
                (create-tables (columns db3)
                               :include-drop include-drop
                               :if-not-exists t)))))

      (cl-postgres::database-errors (e)
        (declare (ignore e))            ; a log has already been printed
        (log-message :fatal "Failed to create the schema, see above.")
        (return-from copy-database)))

    (unless schema-only
      (copy-from db3 :truncate truncate :disable-triggers disable-triggers))

    ;; and report the total time spent on the operation
    (when summary
      (report-full-summary "Total streaming time" *state*
                           :before state-before))))

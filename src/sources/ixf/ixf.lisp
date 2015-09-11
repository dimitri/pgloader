;;;
;;; Tools to handle IBM PC version of IXF file format
;;;
;;; http://www-01.ibm.com/support/knowledgecenter/SSEPGG_10.5.0/com.ibm.db2.luw.admin.dm.doc/doc/r0004667.html

(in-package :pgloader.ixf)

;;;
;;; Integration with pgloader
;;;
(defclass copy-ixf (copy) ()
  (:documentation "pgloader IXF Data Source"))

(defmethod initialize-instance :after ((source copy-ixf) &key)
  "Add a default value for transforms in case it's not been provided."
  (setf (slot-value source 'source)
        (pathname-name (fd-path (source-db source))))

  (with-connection (conn (source-db source))
    (unless (and (slot-boundp source 'columns) (slot-value source 'columns))
      (setf (slot-value source 'columns)
            (list-all-columns (conn-handle conn)
                              (or (typecase (target source)
                                    (cons   (cdr (target source)))
                                    (string (target source)))
                                  (source source)))))

    (let ((transforms (when (slot-boundp source 'transforms)
                        (slot-value source 'transforms))))
      (unless transforms
        (setf (slot-value source 'transforms)
              (loop :for field :in (cdar (columns source))
                 :collect
                 (let ((coltype (cast-ixf-type (ixf:ixf-column-type field))))
                   ;;
                   ;; The IXF driver we use maps the data type and gets
                   ;; back proper CL typed objects, where we only want to
                   ;; deal with text.
                   ;;
                   (cond ((or (string-equal "float" coltype)
                              (string-equal "real" coltype)
                              (string-equal "double precision" coltype)
                              (and (<= 7 (length coltype))
                                   (string-equal "numeric" coltype :end2 7)))
                          #'pgloader.transforms::float-to-string)

                         ((string-equal "text" coltype)
                          nil)

                         ((string-equal "bytea" coltype)
                          #'pgloader.transforms::byte-vector-to-bytea)

                         (t
                          (lambda (c)
                            (when c
                              (princ-to-string c))))))))))))

(defmethod map-rows ((copy-ixf copy-ixf) &key process-row-fn)
  "Extract IXF data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (with-connection (conn (source-db copy-ixf))
    (let ((ixf    (ixf:make-ixf-file :stream (conn-handle conn)))
          (row-fn (lambda (row)
                    (pgstate-incf *state* (target copy-ixf) :read 1)
                    (funcall process-row-fn row))))
      (ixf:read-headers ixf)
      (ixf:map-data ixf row-fn))))

(defmethod copy-to-queue ((ixf copy-ixf) queue)
  "Copy data from IXF file FILENAME into queue DATAQ"
  (let ((read (pgloader.queue:map-push-queue ixf queue)))
    (pgstate-incf *state* (target ixf) :read read)))

(defmethod copy-from ((ixf copy-ixf)
                      &key (kernel nil k-s-p) truncate disable-triggers)
  (let* ((summary        (null *state*))
         (*state*        (or *state* (pgloader.utils:make-pgstate)))
         (lp:*kernel*    (or kernel (make-kernel 2)))
         (channel        (lp:make-channel))
         (queue          (lq:make-queue :fixed-capacity *concurrent-batches*)))

    (with-stats-collection ((target ixf)
                            :dbname (db-name (target-db ixf))
                            :state *state*
                            :summary summary)
      (lp:task-handler-bind ((error #'lp:invoke-transfer-error))
        (log-message :notice "COPY \"~a\" from '~a'" (target ixf) (source ixf))
        (lp:submit-task channel #'copy-to-queue ixf queue)

        ;; and start another task to push that data from the queue to PostgreSQL
        (lp:submit-task channel
                        #'pgloader.pgsql:copy-from-queue
                        (target-db ixf) (target ixf) queue
                        :truncate truncate
                        :disable-triggers disable-triggers)

        ;; now wait until both the tasks are over, and kill the kernel
        (loop for tasks below 2 do (lp:receive-result channel)
           finally
             (log-message :info "COPY \"~a\" done." (target ixf))
             (unless k-s-p (lp:end-kernel)))))))

(defmethod copy-database ((ixf copy-ixf)
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
  "Open the IXF and stream its content to a PostgreSQL database."
  (declare (ignore create-indexes reset-sequences))
  (let* ((summary     (null *state*))
	 (*state*     (or *state* (make-pgstate)))
	 (table-name  (or table-name
			  (target ixf)
			  (source ixf))))

    ;; fix the table-name in the ixf object
    (setf (target ixf) table-name)

    (handler-case
        (when (and (or create-tables schema-only) (not data-only))
          (with-stats-collection ("create, truncate"
                                  :state state-before
                                  :summary summary)
              (with-pgsql-transaction (:pgconn (target-db ixf))
                (when create-tables
                  (with-schema (tname table-name)
                    (log-message :notice "Create table \"~a\"" tname)
                    (create-tables (columns ixf)
                                   :include-drop include-drop
                                   :if-not-exists t))))))

      (cl-postgres::database-error (e)
        (declare (ignore e))            ; a log has already been printed
        (log-message :fatal "Failed to create the schema, see above.")
        (return-from copy-database)))

    (unless schema-only
      (copy-from ixf :truncate truncate :disable-triggers disable-triggers))

    ;; and report the total time spent on the operation
    (when summary
      (report-full-summary "Total streaming time" *state*
                           :before state-before))))


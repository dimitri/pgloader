;;;
;;; Tools to handle IBM PC version of IXF file format
;;;
;;; http://www-01.ibm.com/support/knowledgecenter/SSEPGG_10.5.0/com.ibm.db2.luw.admin.dm.doc/doc/r0004667.html

(in-package :pgloader.ixf)

(defvar *ixf-pgsql-type-mapping*
  '((#. ixf:+smallint+  . "smallint")
    (#. ixf:+integer+   . "integer")
    (#. ixf:+bigint+    . "bigint")

    (#. ixf:+decimal+   . "numeric")
    (#. ixf:+float+     . "double precision")

    (#. ixf:+timestamp+ . "timestamptz")
    (#. ixf:+date+      . "date")
    (#. ixf:+time+      . "time")

    (#. ixf:+char+      . "text")
    (#. ixf:+varchar+   . "text")))

(defun cast-ixf-type (ixf-type)
  "Return the PostgreSQL type name for a given IXF type name."
  (cdr (assoc ixf-type *ixf-pgsql-type-mapping*)))

(defmethod format-pgsql-column ((col ixf:ixf-column))
  "Return a string reprensenting the PostgreSQL column definition"
  (let* ((column-name (apply-identifier-case (ixf:ixf-column-name col)))
         (type-definition
          (format nil
                  "~a~:[ not null~;~]~:[~*~; default ~a~]"
                  (cast-ixf-type (ixf:ixf-column-type col))
                  (ixf:ixf-column-nullable col)
                  (ixf:ixf-column-has-default col)
                  (ixf:ixf-column-default col))))

    (format nil "~a ~22t ~a" column-name type-definition)))

(defun list-all-columns (ixf-file-name
                         &optional (table-name (pathname-name ixf-file-name)))
  "Return the list of columns for the given IXF-FILE-NAME."
  (ixf:with-ixf-file ixf-file-name
    (let ((ixf (ixf:read-headers)))
      (list (cons table-name
                  (coerce (ixf:ixf-table-columns (ixf:ixf-file-table ixf))
                          'list))))))


;;;
;;; Integration with pgloader
;;;
(defclass copy-ixf (copy) ()
  (:documentation "pgloader IXF Data Source"))

(defmethod initialize-instance :after ((source copy-ixf) &key)
  "Add a default value for transforms in case it's not been provided."
  (let* ((fields     (or (and (slot-boundp source 'fields)
			      (slot-value source 'fields))
			 (cdar (list-all-columns (source source)))))

         (transforms (when (slot-boundp source 'transforms)
		       (slot-value source 'transforms))))

    (when fields
      (unless (slot-boundp source 'fields)
	(setf (slot-value source 'fields) fields))

      (unless transforms
	(setf (slot-value source 'transforms)
	      (loop :for field :in fields
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
			    (compile nil (lambda (c)
					   (when c
					     (format nil "~a" c)))))))))))))

(defmethod map-rows ((copy-ixf copy-ixf) &key process-row-fn)
  "Extract IXF data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (ixf:with-ixf-file (source copy-ixf)
    (let ((ixf    (ixf:read-headers))
          (row-fn (lambda (row)
                    (pgstate-incf *state* (target copy-ixf) :read 1)
                    (funcall process-row-fn row))))
      (ixf:map-data ixf row-fn))))

(defmethod copy-to-queue ((ixf copy-ixf) queue)
  "Copy data from IXF file FILENAME into queue DATAQ"
  (let ((read (pgloader.queue:map-push-queue ixf queue)))
    (pgstate-incf *state* (target ixf) :read read)))

(defmethod copy-from ((ixf copy-ixf)
                      &key state-before truncate create-table table-name)
  "Open the IXF and stream its content to a PostgreSQL database."
  (let* ((summary     (null *state*))
	 (*state*     (or *state* (make-pgstate)))
	 (dbname      (target-db ixf))
	 (table-name  (or table-name
			  (target ixf)
			  (pathname-name (source ixf)))))

    ;; fix the table-name in the ixf object
    (setf (target ixf) table-name)

    (with-stats-collection ("create, truncate" :state state-before :summary summary)
      (with-pgsql-transaction ()
	(when create-table
	  (log-message :notice "Create table \"~a\"" table-name)
	  (create-tables (list (cons table-name (fields ixf))) :if-not-exists t))

	(when (and truncate (not create-table))
	  ;; we don't TRUNCATE a table we just CREATEd
	  (let ((truncate-sql  (format nil "TRUNCATE ~a;" table-name)))
	    (log-message :notice "~a" truncate-sql)
	    (pgsql-execute truncate-sql)))))

    (let* ((lp:*kernel*    (make-kernel 2))
	   (channel        (lp:make-channel))
           (queue          (lq:make-queue :fixed-capacity *concurrent-batches*)))

      (with-stats-collection (table-name :state *state* :summary summary)
        (lp:task-handler-bind ((error #'lp:invoke-transfer-error))
          (log-message :notice "COPY \"~a\" from '~a'" (target ixf) (source ixf))
          (lp:submit-task channel #'copy-to-queue ixf queue)

          ;; and start another task to push that data from the queue to PostgreSQL
          (lp:submit-task channel
                          #'pgloader.pgsql:copy-from-queue
                          dbname table-name queue
                          :truncate truncate)

          ;; now wait until both the tasks are over, and kill the kernel
          (loop for tasks below 2 do (lp:receive-result channel)
             finally
               (log-message :info "COPY \"~a\" done." table-name)
               (lp:end-kernel)))))))


;;;
;;; Tools to handle the MS SQL Database
;;;

(in-package :pgloader.mssql)

(defclass copy-mssql (copy)
  ((encoding :accessor encoding         ; allows forcing encoding
             :initarg :encoding
             :initform nil))
  (:documentation "pgloader MS SQL Data Source"))

(defmethod initialize-instance :after ((source copy-mssql) &key)
  "Add a default value for transforms in case it's not been provided."
  (let* ((source-db  (slot-value source 'source-db))
	 (table-name (when (slot-boundp source 'source)
		       (slot-value source 'source)))
	 (fields     (or (and (slot-boundp source 'fields)
			      (slot-value source 'fields))
			 (when table-name
			   (let* ((all-columns (list-all-columns :dbname source-db)))
			     (cdr (assoc table-name all-columns
					 :test #'string=))))))
	 (transforms (when (slot-boundp source 'transforms)
		       (slot-value source 'transforms))))

    ;; default to using the same database name as source and target
    (when (and source-db
	       (or (not (slot-boundp source 'target-db))
		   (not (slot-value source 'target-db))))
      (setf (slot-value source 'target-db) source-db))

    ;; default to using the same table-name as source and target
    (when (and table-name
	       (or (not (slot-boundp source 'target))
                   (not (slot-value source 'target))))
      (setf (slot-value source 'target) table-name))

    (when fields
      (unless (slot-boundp source 'fields)
	(setf (slot-value source 'fields) fields))

      (loop :for field :in fields
         :for (column fn) := (multiple-value-bind (column fn)
                               (cast-mssql-column-definition-to-pgsql field)
                             (list column fn))
         :collect column :into columns
         :collect fn :into fns
         :finally (progn (setf (slot-value source 'columns) columns)
                        (unless transforms
                          (setf (slot-value source 'transforms) fns)))))))

(defmethod map-rows ((mssql copy-mssql) &key process-row-fn)
  "Extract Mssql data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (with-mssql-connection ((source-db mssql))
    (let* ((sql  (destructuring-bind (schema . table-name)
                     (source mssql)
                   (format nil "SELECT ~{~a~^, ~} FROM ~a.~a;"
                           (get-column-list (fields mssql))
                           schema
                           table-name)))
           (row-fn
            (lambda (row)
              (pgstate-incf *state* (target mssql) :read 1)
              (funcall process-row-fn row))))
      (log-message :warning "~a" sql)
      (handler-case
          (mssql::map-query-results sql :row-fn row-fn :connection *mssql-db*)
        (condition (e)
          (progn
            (log-message :error "~a" e)
            (pgstate-incf *state* (target mssql) :errs 1)))))))

(defmethod copy-to-queue ((mssql copy-mssql) queue)
  "Copy data from MSSQL table DBNAME.TABLE-NAME into queue DATAQ"
  (map-push-queue mssql queue))

(defmethod copy-from ((mssql copy-mssql) &key (kernel nil k-s-p) truncate)
  "Connect in parallel to MSSQL and PostgreSQL and stream the data."
  (let* ((summary        (null *state*))
	 (*state*        (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel*    (or kernel (make-kernel 2)))
	 (channel        (lp:make-channel))
	 (queue          (lq:make-queue :fixed-capacity *concurrent-batches*))
	 (table-name     (target mssql)))

    ;; we account stats against the target table-name, because that's all we
    ;; know on the PostgreSQL thread
    (with-stats-collection (table-name :state *state* :summary summary)
      (log-message :notice "COPY ~a" table-name)
      ;; read data from Mssql
      (lp:submit-task channel #'copy-to-queue mssql queue)

      ;; and start another task to push that data from the queue to PostgreSQL
      (lp:submit-task channel #'pgloader.pgsql:copy-from-queue
		      (target-db mssql) (target mssql) queue
		      :truncate truncate)

      ;; now wait until both the tasks are over
      (loop for tasks below 2 do (lp:receive-result channel)
	 finally
	   (log-message :info "COPY ~a done." table-name)
	   (unless k-s-p (lp:end-kernel))))

    ;; return the copy-mssql object we just did the COPY for
    mssql))

(defmethod copy-database ((mssql copy-mssql)
                          &key
			    state-before
			    state-after
			    state-indexes
			    (truncate        nil)
			    (data-only       nil)
			    (schema-only     nil)
			    (create-tables   t)
			    (include-drop    t)
			    (create-indexes  t)
			    (reset-sequences t)
			    (foreign-keys    t)
                            (identifier-case :downcase)
                            (encoding        :utf-8)
			    only-tables)
  "Stream the given MS SQL database down to PostgreSQL."
  (declare (ignore create-indexes reset-sequences foreign-keys))
  (let* ((summary       (null *state*))
	 (*state*       (or *state* (make-pgstate)))
	 (idx-state     (or state-indexes (make-pgstate)))
	 (state-before  (or state-before (make-pgstate)))
	 (state-after   (or state-after   (make-pgstate)))
         (cffi:*default-foreign-encoding* encoding)
         (copy-kernel   (make-kernel 2))
         (all-columns   (filter-column-list
                         (with-mssql-connection ()
                           (list-all-columns))
                         :only-tables only-tables))
         ;; (all-indexes   (filter-column-list (list-all-indexes)
	 ;;        			    :only-tables only-tables
	 ;;        			    :including including
	 ;;        			    :excluding excluding))
         ;; (max-indexes   (loop :for (table . indexes) :in all-indexes
         ;;                   :maximizing (length indexes)))
         ;; (idx-kernel    (when (and max-indexes (< 0 max-indexes))
	 ;;        	  (make-kernel max-indexes)))
         ;; (idx-channel   (when idx-kernel
	 ;;        	  (let ((lp:*kernel* idx-kernel))
	 ;;        	    (lp:make-channel))))
         )

    ;; if asked, first drop/create the tables on the PostgreSQL side
    (cond ((and (or create-tables schema-only) (not data-only))
           (log-message :notice "~:[~;DROP then ~]CREATE TABLES" include-drop)
           (with-stats-collection ("create, truncate"
                                   :state state-before
                                   :summary summary)
             (with-pgsql-transaction ()
               (loop :for (schema . tables) :in all-columns
                  :do (let ((schema
                             (apply-identifier-case schema identifier-case)))
                        ;; create schema
                        (let ((sql (format nil "CREATE SCHEMA ~a;" schema)))
                          (log-message :notice "~a" sql)
                          (pgsql-execute sql))

                        ;; set search_path to only that schema
                        (pgsql-execute
                         (format nil "SET LOCAL search_path TO ~a;" schema))

                        ;; and now create the tables within that schema
                        (create-tables tables
                                       :include-drop include-drop
                                       :identifier-case identifier-case))))))

          (truncate
           (let ((qualified-table-name-list
                  (qualified-table-name-list all-columns
                                             :identifier-case identifier-case)))
             (truncate-tables (target-db mssql)
                              ;; here we really do want only the name
                              (mapcar #'car qualified-table-name-list)
                              :identifier-case identifier-case))))

    ;; Transfert the data
    (loop :for (schema . tables) :in all-columns
       :do (loop :for (table-name . columns) :in tables
              :do
              (let ((table-source
                     (make-instance 'copy-mssql
                                    :source-db (source-db mssql)
                                    :target-db (target-db mssql)
                                    :source    (cons schema table-name)
                                    :target    (qualify-name schema table-name
                                                             :identifier-case
                                                             identifier-case)
                                    :fields    columns)))
                (log-message :debug "TARGET: ~a" (target table-source))
                (log-message :log "target: ~s" table-source)
                ;; COPY the data to PostgreSQL, using copy-kernel
                (unless schema-only
                  (copy-from table-source :kernel copy-kernel)))))

      ;; now end the kernels
    (let ((lp:*kernel* copy-kernel))  (lp:end-kernel))

    ;; and report the total time spent on the operation
    (when summary
      (report-full-summary "Total streaming time" *state*
                           :before state-before
                           :finally state-after
                           :parallel idx-state))))

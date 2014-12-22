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
                   (format nil "SELECT ~{~a~^, ~} FROM [~a].[~a];"
                           (get-column-list (fields mssql))
                           schema
                           table-name)))
           (row-fn
            (lambda (row)
              (pgstate-incf *state* (target mssql) :read 1)
              (funcall process-row-fn row))))
      (log-message :debug "~a" sql)
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
      (lp:task-handler-bind ((error #'lp:invoke-transfer-error))
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
             (unless k-s-p (lp:end-kernel)))))

    ;; return the copy-mssql object we just did the COPY for
    mssql))

(defun complete-pgsql-database (all-columns all-fkeys pkeys
                                &key
                                  state
                                  data-only
                                  foreign-keys
                                  reset-sequences)
    "After loading the data into PostgreSQL, we can now reset the sequences
     and declare foreign keys."
    ;;
    ;; Now Reset Sequences, the good time to do that is once the whole data
    ;; has been imported and once we have the indexes in place, as max() is
    ;; able to benefit from the indexes. In particular avoid doing that step
    ;; while CREATE INDEX statements are in flight (avoid locking).
    ;;
    (when reset-sequences
      (let ((table-names (mapcar #'car (qualified-table-name-list all-columns))))
        (reset-sequences table-names  :state state)))

    ;;
    ;; Turn UNIQUE indexes into PRIMARY KEYS now
    ;;
    (pgstate-add-table state (pgconn-dbname) "Primary Keys")
    (loop :for sql :in pkeys
       :when sql
       :do (progn
             (log-message :notice "~a" sql)
             (pgsql-execute-with-timing (pgconn-dbname) "Primary Keys" sql state)))

    ;;
    ;; Foreign Key Constraints
    ;;
    ;; We need to have finished loading both the reference and the refering
    ;; tables to be able to build the foreign keys, so wait until all tables
    ;; and indexes are imported before doing that.
    ;;
    (when (and foreign-keys (not data-only))
      ;; convert to schema-less list of fkeys
      ;; TODO: fix the MySQL support inherited API
      (let ((all-fkeys
             (loop :for (schema . tables) :in all-fkeys
                :append (loop :for (table-name . fkeys) :in tables
                           :collect (cons table-name (mapcar #'cdr fkeys))))))
        (let ((*identifier-case* :none))
          (create-pgsql-fkeys all-fkeys :state state)))))

(defun fetch-mssql-metadata (&key state including excluding)
  "MS SQL introspection to prepare the migration."
  (let (all-columns all-indexes all-fkeys)
    (with-stats-collection ("fetch meta data"
                            :use-result-as-rows t
                            :use-result-as-read t
                            :state state)
      (with-mssql-connection ()
        (setf all-columns (list-all-columns :including including
                                            :excluding excluding))

        (setf all-indexes (list-all-indexes :including including
                                            :excluding excluding))

        (setf all-fkeys   (list-all-fkeys :including including
                                          :excluding excluding))

        ;; return how many objects we're going to deal with in total
        ;; for stats collection
        (+ (loop :for (schema . tables) :in all-columns :sum (length tables))
           (loop :for (schema . tables) :in all-indexes
              :sum (loop :for (table . indexes) :in tables
                      :sum (length indexes))))))

    ;; now return a plist to the caller
    (list :all-columns all-columns
          :all-indexes all-indexes
          :all-fkeys all-fkeys)))

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
                            (encoding        :utf-8)
                            only-tables
                            including
                            excluding)
  "Stream the given MS SQL database down to PostgreSQL."

  ;; only-tables is part of the generic lambda list, but we don't use it
  ;; here as we didn't implement forcing the schema in the table name, and
  ;; splitting the schema name and table name for processing in list-all-*
  ;; filtering functions
  (declare (ignore only-tables))

  (let* ((summary       (null *state*))
	 (*state*       (or *state* (make-pgstate)))
	 (idx-state     (or state-indexes (make-pgstate)))
	 (state-before  (or state-before  (make-pgstate)))
	 (state-after   (or state-after   (make-pgstate)))
         (cffi:*default-foreign-encoding* encoding)
         (copy-kernel   (make-kernel 2))
         idx-kernel idx-channel)

    (destructuring-bind (&key all-columns all-indexes all-fkeys pkeys)
        ;; to prepare the run we need to fetch MS SQL meta-data
        (fetch-mssql-metadata :state state-before
                              :including including
                              :excluding excluding)

      (let ((max-indexes (loop :for (schema . tables) :in all-indexes
                            :maximizing (loop :for (table . indexes) :in tables
                                           :maximizing (length indexes)))))

        (setf idx-kernel    (when (and max-indexes (< 0 max-indexes))
                              (make-kernel max-indexes)))

        (setf idx-channel   (when idx-kernel
                              (let ((lp:*kernel* idx-kernel))
                                (lp:make-channel)))))

      ;; if asked, first drop/create the tables on the PostgreSQL side
      (handler-case
          (cond ((and (or create-tables schema-only) (not data-only))
                 (log-message :notice "~:[~;DROP then ~]CREATE TABLES" include-drop)
                 (with-stats-collection ("create, truncate"
                                         :state state-before
                                         :summary summary)
                   (with-pgsql-transaction ()
                     (loop :for (schema . tables) :in all-columns
                        :do (let ((schema (apply-identifier-case schema)))
                              ;; create schema
                              (let ((sql (format nil "CREATE SCHEMA ~a;" schema)))
                                (log-message :notice "~a" sql)
                                (pgsql-execute sql))

                              ;; set search_path to only that schema
                              (pgsql-execute
                               (format nil "SET LOCAL search_path TO ~a;" schema))

                              ;; and now create the tables within that schema
                              (create-tables tables :include-drop include-drop)))

                     ;; and set indexes OIDs now
                     ;; TODO: fix the MySQL centric API here
                     (loop :for (schema . tables) :in all-indexes
                        :do (progn
                              (pgsql-execute
                               (format nil "SET LOCAL search_path TO ~a;" schema))
                              (loop :for (table . indexes) :in tables
                                 :for idx := (mapcar #'cdr indexes)
                                 :do (set-table-oids (list (cons table idx)))))))))

                (truncate
                 (let ((qualified-table-name-list
                        (qualified-table-name-list all-columns)))
                   (truncate-tables (target-db mssql)
                                    ;; here we really do want only the name
                                    (mapcar #'car qualified-table-name-list)))))

        ;;
        ;; In case some error happens in the preparatory transaction, we
        ;; need to stop now and refrain from trying to load the data into an
        ;; incomplete schema.
        ;;
        (cl-postgres:database-error (e)
          (declare (ignore e))		; a log has already been printed
          (log-message :fatal "Failed to create the schema, see above.")

          (return-from copy-database)))

      ;; Transfert the data
      (loop :for (schema . tables) :in all-columns
         :do (loop :for (table-name . columns) :in tables
                :do
                (let ((table-source
                       (make-instance 'copy-mssql
                                      :source-db (source-db mssql)
                                      :target-db (target-db mssql)
                                      :source    (cons schema table-name)
                                      :target    (qualify-name schema table-name)
                                      :fields    columns)))

                  (log-message :debug "TARGET: ~a" (target table-source))

                  ;; COPY the data to PostgreSQL, using copy-kernel
                  (unless schema-only
                    (copy-from table-source :kernel copy-kernel))

                  ;; Create the indexes for that table in parallel with the next
                  ;; COPY, and all at once in concurrent threads to benefit from
                  ;; PostgreSQL synchronous scan ability
                  ;;
                  ;; We just push new index build as they come along, if one
                  ;; index build requires much more time than the others our
                  ;; index build might get unsync: indexes for different tables
                  ;; will get built in parallel --- not a big problem.
                  (when (and create-indexes (not data-only))
                    (let* ((*identifier-case* :none)
                           (s-entry  (assoc schema all-indexes :test 'equal))
                           (indexes-with-names
                            (cdr (assoc table-name (cdr s-entry) :test 'equal))))

                      (alexandria:appendf
                       pkeys
                       (create-indexes-in-kernel (target-db mssql)
                                                 (mapcar #'cdr indexes-with-names)
                                                 idx-kernel
                                                 idx-channel
                                                 :state idx-state)))))))

      ;; now end the kernels
      (let ((lp:*kernel* copy-kernel))  (lp:end-kernel))
      (let ((lp:*kernel* idx-kernel))
        ;; wait until the indexes are done being built...
        ;; don't forget accounting for that waiting time.
        (when (and create-indexes (not data-only))
          (with-stats-collection ("Index Build Completion" :state *state*)
            (loop for idx in all-indexes do (lp:receive-result idx-channel))))
        (lp:end-kernel))

      ;;
      ;; Complete the PostgreSQL database before handing over.
      ;;
      (complete-pgsql-database all-columns all-fkeys pkeys
                               :state state-after
                               :data-only data-only
                               :foreign-keys foreign-keys
                               :reset-sequences reset-sequences)

      ;; and report the total time spent on the operation
      (when summary
        (report-full-summary "Total streaming time" *state*
                             :before state-before
                             :finally state-after
                             :parallel idx-state)))))

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
  (let* ((transforms (when (slot-boundp source 'transforms)
		       (slot-value source 'transforms))))
    (when (and (slot-boundp source 'fields) (slot-value source 'fields))
      (loop :for field :in (slot-value source 'fields)
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
  (with-connection (*mssql-db* (source-db mssql))
    (let* ((sql  (destructuring-bind (schema . table-name)
                     (source mssql)
                   (format nil "SELECT ~{~a~^, ~} FROM [~a].[~a];"
                           (get-column-list (fields mssql))
                           schema
                           table-name))))
      (log-message :debug "~a" sql)
      (handler-case
          (handler-bind
              ((condition
                #'(lambda (c)
                    (log-message :error "~a" c)
                    (update-stats :data (target mssql) :errs 1)
                    (invoke-restart 'mssql::use-nil))))
            (mssql::map-query-results sql
                                      :row-fn process-row-fn
                                      :connection (conn-handle *mssql-db*)))
        (condition (e)
          (progn
            (log-message :error "~a" e)
            (update-stats :data (target mssql) :errs 1)))))))

(defun complete-pgsql-database (pgconn all-columns all-fkeys pkeys
                                &key
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
      (reset-sequences table-names :pgconn pgconn)))

  ;;
  ;; Turn UNIQUE indexes into PRIMARY KEYS now
  ;;
  (with-pgsql-connection (pgconn)
    (loop :for sql :in pkeys
       :when sql
       :do (progn
             (log-message :notice "~a" sql)
             (pgsql-execute-with-timing :post "Primary Keys" sql)))

    ;;
    ;; Foreign Key Constraints
    ;;
    ;; We need to have finished loading both the reference and the refering
    ;; tables to be able to build the foreign keys, so wait until all tables
    ;; and indexes are imported before doing that.
    ;;
    (when (and foreign-keys (not data-only))
      (loop :for (schema . tables) :in all-fkeys
         :do (loop :for (table-name . fkeys) :in tables
                :do (loop :for (fk-name . fkey) :in fkeys
                       :for sql := (format-pgsql-create-fkey fkey)
                       :do (progn
                             (log-message :notice "~a;" sql)
                             (pgsql-execute-with-timing :post "Foreign Keys" sql))))))))

(defun fetch-mssql-metadata (mssql &key including excluding)
  "MS SQL introspection to prepare the migration."
  (let (all-columns all-indexes all-fkeys)
    (with-stats-collection ("fetch meta data"
                            :use-result-as-rows t
                            :use-result-as-read t
                            :section :pre)
      (with-connection (*mssql-db* (source-db mssql))
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
			    (truncate         nil)
			    (disable-triggers nil)
			    (data-only        nil)
			    (schema-only      nil)
			    (create-tables    t)
			    (create-schemas   t)
			    (include-drop     t)
			    (create-indexes   t)
			    (reset-sequences  t)
			    (foreign-keys     t)
                            (encoding        :utf-8)
                            including
                            excluding)
  "Stream the given MS SQL database down to PostgreSQL."
  (let* ((cffi:*default-foreign-encoding* encoding)
         (copy-kernel  (make-kernel 2))
         (copy-channel (let ((lp:*kernel* copy-kernel)) (lp:make-channel)))
         (table-count  0)
         idx-kernel idx-channel)

    (destructuring-bind (&key all-columns all-indexes all-fkeys pkeys)
        ;; to prepare the run we need to fetch MS SQL meta-data
        (fetch-mssql-metadata mssql
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
                 (with-stats-collection ("create, truncate" :section :pre)
                   (with-pgsql-transaction (:pgconn (target-db mssql))
                     (loop :for (schema . tables) :in all-columns
                        :do (let ((schema (apply-identifier-case schema)))
                              ;; create schema
                              (when create-schemas
                                (let ((sql (format nil "CREATE SCHEMA ~a;" schema)))
                                  (log-message :notice "~a" sql)
                                  (pgsql-execute sql)))

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

      ;; Transfer the data
      (loop :for (schema . tables) :in all-columns
         :do (loop :for (table-name . columns) :in tables
                :do
                (let ((table-source
                       (make-instance 'copy-mssql
                                      :source-db (clone-connection (source-db mssql))
                                      :target-db (clone-connection (target-db mssql))
                                      :source    (cons schema table-name)
                                      :target    (qualify-name schema table-name)
                                      :fields    columns)))

                  (log-message :debug "TARGET: ~a" (target table-source))

                  ;; COPY the data to PostgreSQL, using copy-kernel
                  (unless schema-only
                    (incf table-count)
                    (copy-from table-source
                               :kernel copy-kernel
                               :channel copy-channel
                               :disable-triggers disable-triggers))

                  ;; Create the indexes for that table in parallel with the next
                  ;; COPY, and all at once in concurrent threads to benefit from
                  ;; PostgreSQL synchronous scan ability
                  ;;
                  ;; We just push new index build as they come along, if one
                  ;; index build requires much more time than the others our
                  ;; index build might get unsync: indexes for different tables
                  ;; will get built in parallel --- not a big problem.
                  (when (and create-indexes (not data-only))
                    (let* ((s-entry  (assoc schema all-indexes :test 'equal))
                           (indexes-with-names
                            (cdr (assoc table-name (cdr s-entry) :test 'equal))))

                      (alexandria:appendf
                       pkeys
                       (create-indexes-in-kernel (target-db mssql)
                                                 (mapcar #'cdr indexes-with-names)
                                                 idx-kernel
                                                 idx-channel)))))))

      ;; now end the kernels
      (let ((lp:*kernel* copy-kernel))
        (with-stats-collection ("COPY Threads Completion" :section :post)
            (loop :for tasks :below (* 2 table-count)
               :do (destructuring-bind (task . table-name)
                       (lp:receive-result copy-channel)
                     (log-message :debug "Finished processing ~a for ~s"
                                  task table-name)))
          (lp:end-kernel)))

      (let ((lp:*kernel* idx-kernel))
        ;; wait until the indexes are done being built...
        ;; don't forget accounting for that waiting time.
        (when (and create-indexes (not data-only))
          (with-stats-collection ("Index Build Completion" :section :post)
            (loop for idx in all-indexes do (lp:receive-result idx-channel))))
        (lp:end-kernel))

      ;;
      ;; Complete the PostgreSQL database before handing over.
      ;;
      (complete-pgsql-database (clone-connection (target-db mssql))
                               all-columns all-fkeys pkeys
                               :data-only data-only
                               :foreign-keys foreign-keys
                               :reset-sequences reset-sequences))))

;;;
;;; Tools to handle MySQL data fetching
;;;

(in-package :pgloader.mysql)

(defclass copy-mysql (copy)
  ((encoding :accessor encoding         ; allows forcing encoding
             :initarg :encoding
             :initform nil))
  (:documentation "pgloader MySQL Data Source"))

(defun cast-mysql-column-definition-to-pgsql (mysql-column)
  "Return the PostgreSQL column definition from the MySQL one."
  (with-slots (table-name name dtype ctype default nullable extra)
      mysql-column
    (cast table-name name dtype ctype default nullable extra)))

(defmethod initialize-instance :after ((source copy-mysql) &key)
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

      (loop for field in fields
         for (column fn) = (multiple-value-bind (column fn)
                               (cast-mysql-column-definition-to-pgsql field)
                             (list column fn))
         collect column into columns
         collect fn into fns
         finally (progn (setf (slot-value source 'columns) columns)
                        (unless transforms
                          (setf (slot-value source 'transforms) fns)))))))


;;;
;;; Implement the specific methods
;;;
(defmethod map-rows ((mysql copy-mysql) &key process-row-fn)
  "Extract MySQL data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (let ((dbname                 (source-db mysql))
	(table-name             (source mysql))
        (qmynd:*mysql-encoding*
         (when (encoding mysql)
           #+sbcl (encoding mysql)
           #+ccl  (ccl:external-format-character-encoding (encoding mysql)))))

    (with-mysql-connection (dbname)
      (when qmynd:*mysql-encoding*
        (log-message :notice "Force encoding to ~a for ~a"
                     qmynd:*mysql-encoding* table-name))
      (let* ((cols (get-column-list dbname table-name))
             (sql  (format nil "SELECT ~{~a~^, ~} FROM `~a`;" cols table-name))
             (row-fn
              (lambda (row)
                (pgstate-incf *state* (target mysql) :read 1)
                (funcall process-row-fn row))))
        (handler-bind
            ;; avoid trying to fetch the character at end-of-input position...
            ((babel-encodings:end-of-input-in-character
              #'(lambda (c)
                  (pgstate-incf *state* (target mysql) :errs 1)
                  (log-message :error "~a" c)
                  (invoke-restart 'qmynd-impl::use-nil)))
             (babel-encodings:character-decoding-error
              #'(lambda (c)
                  (pgstate-incf *state* (target mysql) :errs 1)
                  (let ((encoding (babel-encodings:character-coding-error-encoding c))
                        (position (babel-encodings:character-coding-error-position c))
                        (character
                         (aref (babel-encodings:character-coding-error-buffer c)
                               (babel-encodings:character-coding-error-position c))))
                    (log-message :error
                                 "~a: Illegal ~a character starting at position ~a: ~a."
                                 table-name encoding position character))
                  (invoke-restart 'qmynd-impl::use-nil))))
          (mysql-query sql :row-fn row-fn :result-type 'vector))))))

;;;
;;; Use map-rows and pgsql-text-copy-format to fill in a CSV file on disk
;;; with MySQL data in there.
;;;
(defmethod copy-to ((mysql copy-mysql) filename)
  "Extract data from MySQL in PostgreSQL COPY TEXT format"
  (with-open-file (text-file filename
			     :direction :output
			     :if-exists :supersede
			     :external-format :utf-8)
    (map-rows mysql
	      :process-row-fn
	      (lambda (row)
		(format-vector-row text-file row (transforms mysql))))))

;;;
;;; Export MySQL data to our lparallel data queue. All the work is done in
;;; other basic layers, simple enough function.
;;;
(defmethod copy-to-queue ((mysql copy-mysql) queue)
  "Copy data from MySQL table DBNAME.TABLE-NAME into queue DATAQ"
  (map-push-queue mysql queue))


;;;
;;; Direct "stream" in between mysql fetching of results and PostgreSQL COPY
;;; protocol
;;;
(defmethod copy-from ((mysql copy-mysql) &key (kernel nil k-s-p) truncate)
  "Connect in parallel to MySQL and PostgreSQL and stream the data."
  (let* ((summary        (null *state*))
	 (*state*        (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel*    (or kernel (make-kernel 2)))
	 (channel        (lp:make-channel))
	 (queue          (lq:make-queue :fixed-capacity *concurrent-batches*))
	 (table-name     (target mysql)))

    ;; we account stats against the target table-name, because that's all we
    ;; know on the PostgreSQL thread
    (with-stats-collection (table-name :state *state* :summary summary)
      (log-message :notice "COPY ~a" table-name)
      ;; read data from MySQL
      (lp:submit-task channel #'copy-to-queue mysql queue)

      ;; and start another task to push that data from the queue to PostgreSQL
      (lp:submit-task channel #'pgloader.pgsql:copy-from-queue
		      (target-db mysql) (target mysql) queue
		      :truncate truncate)

      ;; now wait until both the tasks are over
      (loop for tasks below 2 do (lp:receive-result channel)
	 finally
	   (log-message :info "COPY ~a done." table-name)
	   (unless k-s-p (lp:end-kernel))))

    ;; return the copy-mysql object we just did the COPY for
    mysql))


;;;
;;; Prepare the PostgreSQL database before streaming the data into it.
;;;
(defun prepare-pgsql-database (all-columns all-indexes all-fkeys
                               materialize-views view-columns
                               &key
                                 state
                                 identifier-case
                                 foreign-keys
                                 include-drop)
  "Prepare the target PostgreSQL database: create tables casting datatypes
   from the MySQL definitions, prepare index definitions and create target
   tables for materialized views.

   That function mutates index definitions in ALL-INDEXES."
  (log-message :notice "~:[~;DROP then ~]CREATE TABLES" include-drop)
  (log-message :debug  (if include-drop
                           "drop then create ~d tables with ~d indexes."
                           "create ~d tables with ~d indexes.")
               (length all-columns)
               (loop for (name . idxs) in all-indexes sum (length idxs)))

  (with-stats-collection ("create, drop" :use-result-as-rows t :state state)
    (with-pgsql-transaction ()
      ;; we need to first drop the Foreign Key Constraints, so that we
      ;; can DROP TABLE when asked
      (when (and foreign-keys include-drop)
        (drop-pgsql-fkeys all-fkeys :identifier-case identifier-case))

      ;; now drop then create tables and types, etc
      (prog1
          (create-tables all-columns
                         :identifier-case identifier-case
                         :include-drop include-drop)

        ;; MySQL allows the same index name being used against several
        ;; tables, so we add the PostgreSQL table OID in the index name,
        ;; to differenciate. Set the table oids now.
        (set-table-oids all-indexes :identifier-case identifier-case)

        ;; We might have to MATERIALIZE VIEWS
        (when materialize-views
          (create-tables view-columns
                         :identifier-case identifier-case
                         :include-drop include-drop))))))

(defun complete-pgsql-database (all-columns all-fkeys
                                &key
                                  state
                                  data-only
                                  foreign-keys
                                  reset-sequences
                                  identifier-case)
    "After loading the data into PostgreSQL, we can now reset the sequences
     and declare foreign keys."
    ;;
    ;; Now Reset Sequences, the good time to do that is once the whole data
    ;; has been imported and once we have the indexes in place, as max() is
    ;; able to benefit from the indexes. In particular avoid doing that step
    ;; while CREATE INDEX statements are in flight (avoid locking).
    ;;
    (when reset-sequences
      (reset-pgsql-sequences all-columns
                             :state state
                             :identifier-case identifier-case))

    ;;
    ;; Foreign Key Constraints
    ;;
    ;; We need to have finished loading both the reference and the refering
    ;; tables to be able to build the foreign keys, so wait until all tables
    ;; and indexes are imported before doing that.
    ;;
    (when (and foreign-keys (not data-only))
      (create-pgsql-fkeys all-fkeys :state state :identifier-case identifier-case)))

(defun fetch-mysql-metadata (&key
                               state
                               materialize-views
                               only-tables
                               including
                               excluding)
  "MySQL introspection to prepare the migration."
  (let ((view-names    (mapcar #'car materialize-views))
        view-columns all-columns all-fkeys all-indexes)
   (with-stats-collection ("fetch meta data"
                           :use-result-as-rows t
                           :use-result-as-read t
                           :state state)
     (with-mysql-connection ()
       ;; If asked to MATERIALIZE VIEWS, now is the time to create them in
       ;; MySQL, when given definitions rather than existing view names.
       (when materialize-views
         (create-my-views materialize-views))

       (setf all-columns   (filter-column-list (list-all-columns)
                                               :only-tables only-tables
                                               :including including
                                               :excluding excluding)

             all-fkeys     (filter-column-list (list-all-fkeys)
                                               :only-tables only-tables
                                               :including including
                                               :excluding excluding)

             all-indexes   (filter-column-list (list-all-indexes)
                                               :only-tables only-tables
                                               :including including
                                               :excluding excluding)

             view-columns  (when view-names
                             (list-all-columns :only-tables view-names
                                               :table-type :view)))

       ;; return how many objects we're going to deal with in total
       ;; for stats collection
       (+ (length all-columns) (length all-fkeys)
          (length all-indexes) (length view-columns))))

   ;; now return a plist to the caller
   (list :all-columns all-columns
         :all-fkeys all-fkeys
         :all-indexes all-indexes
         :view-columns view-columns)))

(defun apply-decoding-as-filters (table-name filters)
  "Return a generialized boolean which is non-nil only if TABLE-NAME matches
   one of the FILTERS."
  (flet ((apply-filter (filter)
           ;; we close over table-name here.
           (typecase filter
             (string (string-equal filter table-name))
             (list   (destructuring-bind (type val) filter
                       (ecase type
                         (:regex (cl-ppcre:scan val table-name))))))))
    (some #'apply-filter filters)))

;;;
;;; Work on all tables for given database
;;;
(defmethod copy-database ((mysql copy-mysql)
			  &key
			    state-before
			    state-after
			    state-indexes
			    truncate
			    data-only
			    schema-only
			    create-tables
			    include-drop
			    create-indexes
			    reset-sequences
			    foreign-keys
			    (identifier-case :downcase) ; or :quote
			    only-tables
			    including
			    excluding
                            decoding-as
			    materialize-views)
  "Export MySQL data and Import it into PostgreSQL"
  (let* ((summary       (null *state*))
	 (*state*       (or *state*       (make-pgstate)))
	 (idx-state     (or state-indexes (make-pgstate)))
	 (state-before  (or state-before  (make-pgstate)))
	 (state-after   (or state-after   (make-pgstate)))
         (copy-kernel   (make-kernel 2))
	 (dbname        (source-db mysql))
	 (pg-dbname     (target-db mysql))
         idx-kernel idx-channel)

    (destructuring-bind (&key view-columns all-columns all-fkeys all-indexes)
        ;; to prepare the run, we need to fetch MySQL meta-data
        (fetch-mysql-metadata :state state-before
                              :materialize-views materialize-views
                              :only-tables only-tables
                              :including including
                              :excluding excluding)

      ;; prepare our lparallel kernels, dimensioning them to the known sizes
      (let ((max-indexes
             (loop for (table . indexes) in all-indexes
                maximizing (length indexes))))

        (setf idx-kernel    (when (and max-indexes (< 0 max-indexes))
                              (make-kernel max-indexes)))

        (setf idx-channel   (when idx-kernel
                              (let ((lp:*kernel* idx-kernel))
                                (lp:make-channel)))))

      ;; if asked, first drop/create the tables on the PostgreSQL side
      (handler-case
          (cond ((and (or create-tables schema-only) (not data-only))
                 (prepare-pgsql-database all-columns
                                         all-indexes
                                         all-fkeys
                                         materialize-views
                                         view-columns
                                         :state state-before
                                         :foreign-keys foreign-keys
                                         :identifier-case identifier-case
                                         :include-drop include-drop))
                (t
                 (when truncate
                   (truncate-tables *pg-dbname*
                                    (mapcar #'car all-columns)
                                    :identifier-case identifier-case))))
        ;;
        ;; In case some error happens in the preparatory transaction, we
        ;; need to stop now and refrain from trying to load the data into
        ;; an incomplete schema.
        ;;
        (cl-postgres:database-error (e)
          (declare (ignore e))		; a log has already been printed
          (log-message :fatal "Failed to create the schema, see above.")
          (return-from copy-database)))

      (loop
         for (table-name . columns) in (append all-columns view-columns)

         unless columns
         do (log-message :error "Table ~s not found, skipping." table-name)

         when columns
         do
           (let* ((encoding
                   ;; force the data encoding when asked to
                   (when decoding-as
                     (loop :for (encoding . filters) :in decoding-as
                        :when (apply-decoding-as-filters table-name filters)
                        :return encoding)))

                  (table-source
                   (make-instance 'copy-mysql
                                  :source-db  dbname
                                  :target-db  pg-dbname
                                  :source     table-name
                                  :target     (apply-identifier-case table-name
                                                                     identifier-case)
                                  :fields     columns
                                  :encoding   encoding)))

             (log-message :debug "TARGET: ~a" (target table-source))

             ;; first COPY the data from MySQL to PostgreSQL, using copy-kernel
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
               (let* ((indexes
                       (cdr (assoc table-name all-indexes :test #'string=))))
                 (create-indexes-in-kernel pg-dbname indexes
                                           idx-kernel idx-channel
                                           :state idx-state
                                           :identifier-case identifier-case)))))

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
      ;; If we created some views for this run, now is the time to DROP'em
      ;;
      (when materialize-views
        (with-mysql-connection ()
          (drop-my-views materialize-views)))

      ;;
      ;; Complete the PostgreSQL database before handing over.
      ;;
      (complete-pgsql-database all-columns all-fkeys
                               :state state-after
                               :data-only data-only
                               :foreign-keys foreign-keys
                               :reset-sequences reset-sequences
                               :identifier-case identifier-case)

      ;; and report the total time spent on the operation
      (when summary
        (report-full-summary "Total streaming time" *state*
                             :before   state-before
                             :finally  state-after
                             :parallel idx-state)))))

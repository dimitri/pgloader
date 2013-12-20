;;;
;;; Tools to handle MySQL data fetching
;;;

(in-package :pgloader.mysql)

(defclass copy-mysql (copy) ()
  (:documentation "pgloader MySQL Data Source"))

(defmethod initialize-instance :after ((source copy-mysql) &key)
  "Add a default value for transforms in case it's not been provided."
  (let* ((source-db  (slot-value source 'source-db))
	 (table-name (when (slot-boundp source 'source)
		       (slot-value source 'source)))
	 (fields     (or (and (slot-boundp source 'fields)
			      (slot-value source 'fields))
			 (when table-name
			   (let* ((all-columns (list-all-columns source-db)))
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

      (unless transforms
	(setf (slot-value source 'transforms) (list-transforms fields))))))


;;;
;;; Implement the specific methods
;;;
(defmethod map-rows ((mysql copy-mysql) &key process-row-fn)
  "Extract MySQL data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (let ((dbname     (source-db mysql))
	(table-name (source mysql)))

    (with-mysql-connection (dbname)
      (mysql-query "SET NAMES 'utf8'")
      (mysql-query "SET character_set_results = utf8;")

      (let* ((cols (get-column-list dbname table-name))
             (sql  (format nil "SELECT ~{~a~^, ~} FROM `~a`;" cols table-name))
             (row-fn
              (lambda (row)
                (pgstate-incf *state* (target mysql) :read 1)
                (funcall process-row-fn row))))
        (handler-bind
            ((babel-encodings:character-decoding-error
              #'(lambda (e)
                  (pgstate-incf *state* (target mysql) :errs 1)
                  (log-message :error "~a" e))))
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
		(pgloader.pgsql:format-row text-file row
					   :transforms (transforms mysql))))))

;;;
;;; Export MySQL data to our lparallel data queue. All the work is done in
;;; other basic layers, simple enough function.
;;;
(defmethod copy-to-queue ((mysql copy-mysql) dataq)
  "Copy data from MySQL table DBNAME.TABLE-NAME into queue DATAQ"
  (pgloader.queue:map-push-queue dataq #'map-rows mysql))


;;;
;;; Direct "stream" in between mysql fetching of results and PostgreSQL COPY
;;; protocol
;;;
(defmethod copy-from ((mysql copy-mysql) &key (kernel nil k-s-p) truncate)
  "Connect in parallel to MySQL and PostgreSQL and stream the data."
  (let* ((summary     (null *state*))
	 (*state*     (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel* (or kernel (make-kernel 2)))
	 (channel     (lp:make-channel))
	 (dataq       (lq:make-queue :fixed-capacity 4096))
	 (dbname      (source-db mysql))
	 (table-name  (target mysql)))

    ;; we account stats against the target table-name, because that's all we
    ;; know on the PostgreSQL thread
    (with-stats-collection (dbname table-name :state *state* :summary summary)
      (log-message :notice "COPY ~a" table-name)
      ;; read data from MySQL
      (lp:submit-task channel #'copy-to-queue mysql dataq)

      ;; and start another task to push that data from the queue to PostgreSQL
      (lp:submit-task channel
		      #'pgloader.pgsql:copy-from-queue
		      (target-db mysql)
		      (target mysql)
		      dataq
		      :truncate truncate
		      :transforms (transforms mysql))

      ;; now wait until both the tasks are over
      (loop for tasks below 2 do (lp:receive-result channel)
	 finally
	   (log-message :info "COPY ~a done." table-name)
	   (unless k-s-p (lp:end-kernel))))

    ;; return the copy-mysql object we just did the COPY for
    mysql))


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
	 (view-names    (mapcar #'car materialize-views))

         ;; all to be set within a single MySQL transaction
	 view-columns all-columns all-fkeys all-indexes

         ;; those depend on the previous entries
         idx-kernel idx-channel)

    ;; to prepare the run, we need to fetch MySQL meta-data
    (with-stats-collection (pg-dbname "fetch meta data" :state state-before)
     (with-mysql-connection (dbname)
       ;; If asked to materialize views, now is the time to create
       ;; the target tables for them
       (when materialize-views
         (create-my-views dbname materialize-views))

       (setf all-columns   (filter-column-list (list-all-columns dbname)
                                               :only-tables only-tables
                                               :including including
                                               :excluding excluding)

             all-fkeys     (filter-column-list (list-all-fkeys dbname)
                                               :only-tables only-tables
                                               :including including
                                               :excluding excluding)

             all-indexes   (filter-column-list (list-all-indexes dbname)
                                               :only-tables only-tables
                                               :including including
                                               :excluding excluding)

             view-columns  (list-all-columns dbname
                                             :only-tables view-names
                                             :table-type :view))))

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
    (when (and (or create-tables schema-only) (not data-only))
      (log-message :notice "~:[~;DROP then ~]CREATE TABLES" include-drop)
      (log-message :debug  (if include-drop
			       "drop then create ~d tables with ~d indexes."
			       "create ~d tables with ~d indexes.")
		   (length all-columns)
		   (loop for (name . idxs) in all-indexes sum (length idxs)))
      (with-stats-collection (pg-dbname "create, drop"
					:use-result-as-rows t
					:state state-before)
	(handler-case
	    (with-pgsql-transaction (pg-dbname)
	      ;; we need to first drop the Foreign Key Constraints, so that we
	      ;; can DROP TABLE when asked
	      (when (and foreign-keys include-drop)
		(drop-fkeys all-fkeys
			    :dbname pg-dbname
			    :identifier-case identifier-case))

	      ;; now drop then create tables and types, etc
	      (create-tables all-columns
			     :identifier-case identifier-case
			     :include-drop include-drop)

	      ;; MySQL allows the same index name being used against several
	      ;; tables, so we add the PostgreSQL table OID in the index name,
	      ;; to differenciate. Set the table oids now.
	      (set-table-oids all-indexes
                              :identifier-case identifier-case)

              ;; We might have to MATERIALIZE VIEWS
              (when materialize-views
                (create-tables view-columns
                               :identifier-case identifier-case
                               :include-drop include-drop)))

	  ;;
	  ;; In case some error happens in the preparatory transaction, we
	  ;; need to stop now and refrain to try loading the data into an
	  ;; incomplete schema.
	  ;;
	  (qmynd:mysql-error (e)
	    (log-message :fatal "~a" e)
	    (return-from copy-database))

	  (cl-postgres:database-error (e)
	    (declare (ignore e))		; a log has already been printed
	    (log-message :fatal "Failed to create the schema, see above.")
	    (return-from copy-database)))))

    (loop
       for (table-name . columns) in (append all-columns view-columns)
       do
	 (let ((table-source
		(make-instance 'copy-mysql
			       :source-db  dbname
			       :target-db  pg-dbname
			       :source     table-name
			       :target     (apply-identifier-case table-name
                                                                  identifier-case)
			       :fields     columns)))
           (log-message :debug "TARGET: ~a" (target table-source))
	   ;; first COPY the data from MySQL to PostgreSQL, using copy-kernel
	   (unless schema-only
	     (copy-from table-source :kernel copy-kernel :truncate truncate))

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
	(with-stats-collection (pg-dbname "Index Build Completion" :state *state*)
	  (loop for idx in all-indexes do (lp:receive-result idx-channel))))
      (lp:end-kernel))

    ;;
    ;; If we created some views for this run, now is the time to DROP'em
    ;;
    (when materialize-views
      (with-mysql-connection (dbname)
        (drop-my-views dbname materialize-views)))
    ;;
    ;; Now Reset Sequences, the good time to do that is once the whole data
    ;; has been imported and once we have the indexes in place, as max() is
    ;; able to benefit from the indexes. In particular avoid doing that step
    ;; while CREATE INDEX statements are in flight (avoid locking).
    ;;
    (when reset-sequences
      (reset-sequences all-columns
		       :dbname pg-dbname
		       :state state-after
		       :identifier-case identifier-case))

    ;;
    ;; Foreign Key Constraints
    ;;
    ;; We need to have finished loading both the reference and the refering
    ;; tables to be able to build the foreign keys, so wait until all tables
    ;; and indexes are imported before doing that.
    ;;
    (when (and foreign-keys (not data-only))
      (create-fkeys all-fkeys
		    :dbname pg-dbname
		    :state state-after
		    :identifier-case identifier-case))

    ;; and report the total time spent on the operation
    (when summary
      (report-full-summary "Total streaming time" *state*
			   :before   state-before
			   :finally  state-after
			   :parallel idx-state))))


;;;
;;; MySQL bulk export to file, in PostgreSQL COPY TEXT format
;;;
(defun export-database (dbname &key only-tables)
  "Export MySQL tables into as many TEXT files, in the PostgreSQL COPY format"
  (let ((all-columns  (list-all-columns dbname)))
    (setf *state* (pgloader.utils:make-pgstate))
    (report-header)
    (loop
       for (table-name . cols) in all-columns
       for filename = (pgloader.csv:get-pathname dbname table-name)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgstate-add-table *state* dbname table-name)
	 (report-table-name table-name)
	 (multiple-value-bind (rows secs)
	     (timing
	      ;; load data
	      (let ((source
		     (make-instance 'copy-mysql
				    :source-db dbname
				    :source table-name
				    :fields cols)))
		(copy-to source filename)))
	   ;; update and report stats
	   (pgstate-incf *state* table-name :read rows :secs secs)
	   (report-pgtable-stats *state* table-name))
       finally
	 (report-pgstate-stats *state* "Total export time"))))

;;;
;;; Copy data from a target database into files in the PostgreSQL COPY TEXT
;;; format, then load those files. Useful mainly to compare timing with the
;;; direct streaming method. If you need to pre-process the files, use
;;; export-database, do the extra processing, then use
;;; pgloader.pgsql:copy-from-file on each file.
;;;
(defun export-import-database (dbname
			       &key
				 (pg-dbname dbname)
				 (truncate t)
				 only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  ;; get the list of tables and have at it
  (let ((all-columns  (list-all-columns dbname)))
    (setf *state* (pgloader.utils:make-pgstate))
    (report-header)
    (loop
       for (table-name . cols) in all-columns
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgstate-add-table *state* dbname table-name)
	 (report-table-name table-name)

	 (multiple-value-bind (res secs)
	     (timing
	      (let* ((filename (pgloader.csv:get-pathname dbname table-name)))
		;; export from MySQL to file
		(let ((source
		       (make-instance 'copy-mysql
				      :source-db dbname
				      :source table-name
				      :fields cols)))
		  (copy-to source filename))
		;; import the file to PostgreSQL
		(pgloader.pgsql:copy-from-file pg-dbname
					       table-name
					       filename
					       :truncate truncate)))
	   (declare (ignore res))
	   (pgstate-incf *state* table-name :secs secs)
	   (report-pgtable-stats *state* table-name))
       finally
	 (report-pgstate-stats *state* "Total export+import time"))))

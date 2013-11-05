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
		   (slot-value source 'target)))
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

    (cl-mysql:connect :host *myconn-host*
		      :port *myconn-port*
		      :user *myconn-user*
		      :password *myconn-pass*)

    (unwind-protect
	 (progn
	   ;; Ensure we're talking utf-8 and connect to DBNAME in MySQL
	   (cl-mysql:query "SET NAMES 'utf8'")
	   (cl-mysql:query "SET character_set_results = utf8;")
	   (cl-mysql:use dbname)

	   (multiple-value-bind (cols nulls)
	       (get-column-list-with-is-nulls dbname table-name)
	     (let* ((sql  (format nil "SELECT ~{~a~^, ~} FROM ~a;" cols table-name))
		    (q    (cl-mysql:query sql :store nil :type-map nil))
		    (rs   (cl-mysql:next-result-set q)))
	       (declare (ignore rs))

	       ;; Now fetch MySQL rows directly in the stream
	       (loop
		  with type-map = (make-hash-table)
		  for row = (cl-mysql:next-row q :type-map type-map)
		  while row
		  for row-with-proper-nulls = (fix-nulls row nulls)
		  counting row into count
		  do (funcall process-row-fn row-with-proper-nulls)
		  finally (return count)))))

      ;; free resources
      (cl-mysql:disconnect))))

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
  (let ((read (pgloader.queue:map-push-queue dataq #'map-rows mysql)))
    (pgstate-incf *state* (target mysql) :read read)))


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
	 (table-name  (source mysql)))

    (with-stats-collection (dbname table-name :state *state* :summary summary)
      (log-message :notice "COPY ~a.~a" dbname table-name)
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
	   (log-message :info "COPY ~a.~a done." dbname table-name)
	   (unless k-s-p (lp:end-kernel))))))


;;;
;;; Work on all tables for given database
;;;
(defun create-indexes-in-kernel (dbname table-name indexes kernel channel
				 &key
				   identifier-case include-drop
				   state (label "create index"))
  "Create indexes for given table in dbname, using given lparallel KERNEL
   and CHANNEL so that the index build happen in concurrently with the data
   copying."
  (let* ((lp:*kernel* kernel)
	 (table-oid
	  (with-pgsql-transaction (dbname)
	    (pomo:query
	     (format nil "select '~a'::regclass::oid" table-name) :single))))
    (pgstate-add-table state dbname label)

    (when include-drop
      (let ((drop-channel (lp:make-channel))
	    (drop-indexes
	     (drop-index-sql-list table-name table-oid indexes
				  :identifier-case identifier-case)))
	(loop
	   for sql in drop-indexes
	   do
	     (log-message :notice "~a" sql)
	     (lp:submit-task drop-channel
			     #'pgsql-execute-with-timing
			     dbname label sql state))

	;; wait for the DROP INDEX to be done before issuing CREATE INDEX
	(loop for idx in drop-indexes do (lp:receive-result drop-channel))))

    (loop
       for sql in (create-index-sql-list table-name table-oid indexes
					 :identifier-case identifier-case)
       do
	 (log-message :notice "~a" sql)
	 (lp:submit-task channel #'pgsql-execute-with-timing
			 dbname label sql state))))

(defmethod copy-database ((mysql copy-mysql)
			  &key
			    truncate
			    schema-only
			    create-tables
			    include-drop
			    create-indexes
			    reset-sequences
			    (identifier-case :downcase) ; or :quote
			    only-tables
			    including
			    excluding)
  "Export MySQL data and Import it into PostgreSQL"
  (let* ((*state*       (make-pgstate))
	 (idx-state     (make-pgstate))
	 (seq-state     (make-pgstate))
         (copy-kernel   (make-kernel 2))
	 (dbname        (source-db mysql))
	 (pg-dbname     (target-db mysql))
         (all-columns   (filter-column-list (list-all-columns dbname)
					    :only-tables only-tables
					    :including including
					    :excluding excluding))
         (all-indexes   (filter-column-list (list-all-indexes dbname)
					    :only-tables only-tables
					    :including including
					    :excluding excluding))
         (max-indexes   (loop for (table . indexes) in all-indexes
                           maximizing (length indexes)))
         (idx-kernel    (when (and max-indexes (< 0 max-indexes))
			  (make-kernel max-indexes)))
         (idx-channel   (when idx-kernel
			  (let ((lp:*kernel* idx-kernel))
			    (lp:make-channel)))))

    ;; if asked, first drop/create the tables on the PostgreSQL side
    (when create-tables
      (log-message :notice "~:[~;DROP then ~]CREATE TABLES" include-drop)
      (with-pgsql-transaction (pg-dbname)
	(create-tables all-columns
		       :identifier-case identifier-case
		       :include-drop include-drop)))

    (loop
       for (table-name . columns) in all-columns
       do
	 (let ((table-source
		(make-instance 'copy-mysql
			       :source-db  dbname
			       :target-db  pg-dbname
			       :source     table-name
			       :target     table-name
			       :fields     columns)))
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
	   (when create-indexes
	     (let* ((indexes
		     (cdr (assoc table-name all-indexes :test #'string=))))
	       (create-indexes-in-kernel pg-dbname table-name indexes
					 idx-kernel idx-channel
					 :state idx-state
					 :include-drop include-drop
					 :identifier-case identifier-case)))))

    ;; don't forget to reset sequences, but only when we did actually import
    ;; the data.
    (when (and (not schema-only) reset-sequences)
      (let ((tables
	     (mapcar
	      (lambda (name) (apply-identifier-case name identifier-case))
	      (or only-tables
		  (mapcar #'car all-columns)))))
	(log-message :notice "Reset sequences")
	(with-stats-collection (pg-dbname "reset sequences"
					  :use-result-as-rows t
					  :state seq-state)
	  (pgloader.pgsql:reset-all-sequences pg-dbname :tables tables))))

    ;; now end the kernels
    (let ((lp:*kernel* copy-kernel))  (lp:end-kernel))
    (let ((lp:*kernel* idx-kernel))
      ;; wait until the indexes are done being built...
      ;; don't forget accounting for that waiting time.
      (with-stats-collection (pg-dbname "index build completion" :state *state*)
	(loop for idx in all-indexes do (lp:receive-result idx-channel)))
      (lp:end-kernel))

    ;; and report the total time spent on the operation
    (report-summary)
    (format t pgloader.utils::*header-line*)
    (report-summary :state idx-state :header nil :footer nil)
    (report-summary :state seq-state :header nil :footer nil)
    ;; don't forget to add up the RESET SEQUENCES timings
    (incf (pgloader.utils::pgstate-secs *state*)
	  (pgloader.utils::pgstate-secs seq-state))
    (report-pgstate-stats *state* "Total streaming time")))


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

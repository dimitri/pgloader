;;;
;;; Tools to handle the SQLite Database
;;;

(in-package :pgloader.sqlite)

(defvar *sqlite-db* nil
  "The SQLite database connection handler.")

;;;
;;; SQLite tools connecting to a database
;;;
(defstruct (coldef
	     (:constructor make-coldef (seq name type nullable default pk-id)))
  seq name type nullable default pk-id)

(defun cast (sqlite-type-name)
  "Return the PostgreSQL type name for a given SQLite type name."
  (cond ((and (<= 8 (length sqlite-type-name))
	      (string-equal sqlite-type-name "nvarchar" :end1 8)) "text")

	((string-equal sqlite-type-name "datetime") "timestamptz")
        ((string-equal sqlite-type-name "double")   "double precision")
        ((string-equal sqlite-type-name "blob")     "bytea")

	(t sqlite-type-name)))

(defmethod format-pgsql-column ((col coldef) &key identifier-case)
  "Return a string representing the PostgreSQL column definition."
  (let* ((column-name
	  (apply-identifier-case (coldef-name col) identifier-case))
	 (type-definition
	  (format nil
		  "~a~:[~; not null~]~@[ default ~a~]"
		  (cast (coldef-type col))
		  (coldef-nullable col)
		  (coldef-default col))))
    (format nil "~a ~22t ~a" column-name type-definition)))

(defun list-tables (&optional (db *sqlite-db*))
  "Return the list of tables found in SQLITE-DB."
  (let ((sql "SELECT tbl_name
                FROM sqlite_master
               WHERE type='table' AND tbl_name <> 'sqlite_sequence'"))
    (loop for (name) in (sqlite:execute-to-list db sql)
       collect name)))

(defun list-columns (table-name &optional (db *sqlite-db*))
  "Return the list of columns found in TABLE-NAME."
  (let ((sql (format nil "PRAGMA table_info(~a)" table-name)))
    (loop for (seq name type nullable default pk-id) in
	 (sqlite:execute-to-list db sql)
       collect (make-coldef seq name type (= 1 nullable) default pk-id))))

(defun list-all-columns (&optional (db *sqlite-db*))
  "Get the list of SQLite column definitions per table."
  (loop for table-name in (list-tables db)
     collect (cons table-name (list-columns table-name db))))

(defstruct sqlite-idx name table-name sql)

(defmethod index-table-name ((index sqlite-idx))
  (sqlite-idx-table-name index))

(defmethod format-pgsql-create-index ((index sqlite-idx) &key identifier-case)
  "Generate the PostgresQL statement to build the given SQLite index definition."
  (declare (ignore identifier-case))
  (sqlite-idx-sql index))

(defun list-all-indexes (&optional (db *sqlite-db*))
  "Get the list of SQLite index definitions per table."
  (let ((sql "SELECT name, tbl_name, replace(replace(sql, '[', ''), ']', '')
                FROM sqlite_master
               WHERE type='index'"))
    (loop :with schema := nil
       :for (index-name table-name sql) :in (sqlite:execute-to-list db sql)
       :when sql
       :do (let ((entry  (assoc table-name schema :test 'equal))
                 (idxdef (make-sqlite-idx :name index-name
                                          :table-name table-name
                                          :sql sql)))
             (if entry
                 (push idxdef (cdr entry))
                 (push (cons table-name (list idxdef)) schema)))
       :finally (return (reverse (loop for (name . indexes) in schema
                                    collect (cons name (reverse indexes))))))))


;;;
;;; Integration with the pgloader Source API
;;;
(defclass copy-sqlite (copy)
  ((db :accessor db :initarg :db))
  (:documentation "pgloader SQLite Data Source"))

(defmethod initialize-instance :after ((source copy-sqlite) &key)
  "Add a default value for transforms in case it's not been provided."
  (let* ((source-db  (slot-value source 'source-db))
	 (db         (sqlite:connect (get-absolute-pathname `(:filename ,source-db))))
	 (table-name (when (slot-boundp source 'source)
		       (slot-value source 'source)))
	 (fields     (or (and (slot-boundp source 'fields)
			      (slot-value source 'fields))
			 (when table-name
			   (list-columns table-name db))))
	 (transforms (when (slot-boundp source 'transforms)
		       (slot-value source 'transforms))))

    ;; we will reuse the same SQLite database handler that we just opened
    (setf (slot-value source 'db) db)

    ;; default to using the same table-name as source and target
    (when (and table-name
	       (or (not (slot-boundp source 'target))
		   (slot-value source 'target)))
      (setf (slot-value source 'target) table-name))

    (when fields
      (unless (slot-boundp source 'fields)
	(setf (slot-value source 'fields) fields))

      (unless transforms
	(setf (slot-value source 'transforms)
	      (loop for field in fields
		 collect
		   (let ((coltype (cast (coldef-type field))))
		     ;;
		     ;; The SQLite drive we use maps the CFFI data type
		     ;; mapping functions and gets back proper CL typed
		     ;; objects, where we only want to deal with text.
		     ;;
		     (cond ((or (string-equal "float" coltype)
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

;;; Map a function to each row extracted from SQLite
;;;
(defmethod map-rows ((sqlite copy-sqlite) &key process-row-fn)
  "Extract SQLite data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row"
  (let ((sql (format nil "SELECT * FROM ~a" (source sqlite))))
   (loop
      with statement = (sqlite:prepare-statement (db sqlite) sql)
      with len = (loop :for name :in (sqlite:statement-column-names statement)
                    :count name)
      while (sqlite:step-statement statement)
      for row = (let ((v (make-array len)))
                  (loop :for x :below len
                     :do (setf (aref v x)
                               (sqlite:statement-column-value statement x)))
                  v)
      counting t into rows
      do (funcall process-row-fn row)
      finally
        (sqlite:finalize-statement statement)
        (return rows))))


(defmethod copy-to-queue ((sqlite copy-sqlite) queue)
  "Copy data from SQLite table TABLE-NAME within connection DB into queue DATAQ"
  (let ((read (pgloader.queue:map-push-queue sqlite queue)))
    (pgstate-incf *state* (target sqlite) :read read)))

(defmethod copy-from ((sqlite copy-sqlite) &key (kernel nil k-s-p) truncate)
  "Stream the contents from a SQLite database table down to PostgreSQL."
  (let* ((summary     (null *state*))
	 (*state*     (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel* (or kernel (make-kernel 2)))
	 (channel     (lp:make-channel))
	 (queue       (lq:make-queue :fixed-capacity *concurrent-batches*))
	 (table-name  (target sqlite))
	 (pg-dbname   (target-db sqlite)))

    (with-stats-collection (table-name :state *state* :summary summary)
      (log-message :notice "COPY ~a" table-name)
      ;; read data from SQLite
      (lp:submit-task channel #'copy-to-queue sqlite queue)

      ;; and start another task to push that data from the queue to PostgreSQL
      (lp:submit-task channel
		      #'pgloader.pgsql:copy-from-queue
		      pg-dbname table-name queue
		      :truncate truncate)

      ;; now wait until both the tasks are over
      (loop for tasks below 2 do (lp:receive-result channel)
	 finally
	   (log-message :info "COPY ~a done." table-name)
	   (unless k-s-p (lp:end-kernel))))))

(defmethod copy-database ((sqlite copy-sqlite)
			  &key
			    state-before
			    truncate
			    data-only
			    schema-only
			    create-tables
			    include-drop
			    create-indexes
			    reset-sequences
			    only-tables
			    including
			    excluding)
  "Stream the given SQLite database down to PostgreSQL."
  (let* ((summary       (null *state*))
	 (*state*       (or *state* (make-pgstate)))
	 (state-before  (or state-before (make-pgstate)))
	 (idx-state     (make-pgstate))
	 (seq-state     (make-pgstate))
         (copy-kernel   (make-kernel 2))
         (all-columns   (filter-column-list (list-all-columns (db sqlite))
					    :only-tables only-tables
					    :including including
					    :excluding excluding))
         (all-indexes   (filter-column-list (list-all-indexes (db sqlite))
					    :only-tables only-tables
					    :including including
					    :excluding excluding))
         (max-indexes   (loop for (table . indexes) in all-indexes
                           maximizing (length indexes)))
         (idx-kernel    (when (and max-indexes (< 0 max-indexes))
			  (make-kernel max-indexes)))
         (idx-channel   (when idx-kernel
			  (let ((lp:*kernel* idx-kernel))
			    (lp:make-channel))))
	 (pg-dbname     (target-db sqlite)))

    ;; if asked, first drop/create the tables on the PostgreSQL side
    (when (and (or create-tables schema-only) (not data-only))
      (log-message :notice "~:[~;DROP then ~]CREATE TABLES" include-drop)
      (with-stats-collection ("create, truncate"
                              :state state-before
                              :summary summary)
	(with-pgsql-transaction ()
	  (create-tables all-columns :include-drop include-drop))))

    (loop
       for (table-name . columns) in all-columns
       do
	 (let ((table-source
		(make-instance 'copy-sqlite
			       :db         (db sqlite)
			       :source-db  (source-db sqlite)
			       :target-db  pg-dbname
			       :source     table-name
			       :target     table-name
			       :fields     columns)))
	   ;; first COPY the data from SQLite to PostgreSQL, using copy-kernel
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
					 :state idx-state)))))

    ;; don't forget to reset sequences, but only when we did actually import
    ;; the data.
    (when reset-sequences
      (let ((tables (or only-tables
			(mapcar #'car all-columns))))
	(log-message :notice "Reset sequences")
	(with-stats-collection ("reset sequences"
                                :use-result-as-rows t
                                :state seq-state)
	  (pgloader.pgsql:reset-all-sequences pg-dbname :tables tables))))

    ;; now end the kernels
    (let ((lp:*kernel* copy-kernel))  (lp:end-kernel))
    (let ((lp:*kernel* idx-kernel))
      ;; wait until the indexes are done being built...
      ;; don't forget accounting for that waiting time.
      (when (and create-indexes (not data-only))
	(with-stats-collection ("index build completion" :state *state*)
	 (loop for idx in all-indexes do (lp:receive-result idx-channel))))
      (lp:end-kernel))

    ;; and report the total time spent on the operation
    (report-summary :state state-before)
    (report-summary :header nil :footer nil)
    (format t pgloader.utils::*header-line*)
    (report-summary :state idx-state :header nil :footer nil)
    (report-summary :state seq-state :header nil :footer nil)
    ;; don't forget to add up the RESET SEQUENCES timings
    (incf (pgloader.utils::pgstate-secs *state*)
	  (pgloader.utils::pgstate-secs seq-state))
    (report-pgstate-stats *state* "Total streaming time")))


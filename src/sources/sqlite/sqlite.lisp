;;;
;;; Tools to handle the SQLite Database
;;;

(in-package :pgloader.sqlite)

;;;
;;; Integration with the pgloader Source API
;;;
(defclass sqlite-connection (fd-connection) ())

(defmethod initialize-instance :after ((slconn sqlite-connection) &key)
  "Assign the type slot to sqlite."
  (setf (slot-value slconn 'type) "sqlite"))

(defmethod open-connection ((slconn sqlite-connection) &key)
  (setf (conn-handle slconn)
        (sqlite:connect (fd-path slconn)))
  (log-message :debug "CONNECTED TO ~a" (fd-path slconn))
  slconn)

(defmethod close-connection ((slconn sqlite-connection))
  (sqlite:disconnect (conn-handle slconn))
  (setf (conn-handle slconn) nil)
  slconn)

(defmethod query ((slconn sqlite-connection) sql &key)
  (sqlite:execute-to-list (conn-handle slconn) sql))

(defclass copy-sqlite (copy)
  ((db :accessor db :initarg :db))
  (:documentation "pgloader SQLite Data Source"))

(defmethod initialize-instance :after ((source copy-sqlite) &key)
  "Add a default value for transforms in case it's not been provided."
  (let* ((transforms (when (slot-boundp source 'transforms)
		       (slot-value source 'transforms))))
    (when (and (slot-boundp source 'fields) (slot-value source 'fields))
      (loop for field in (slot-value source 'fields)
         for (column fn) = (multiple-value-bind (column fn)
                               (cast-sqlite-column-definition-to-pgsql field)
                             (list column fn))
         collect column into columns
         collect fn into fns
         finally (progn (setf (slot-value source 'columns) columns)
                        (unless transforms
                          (setf (slot-value source 'transforms) fns)))))))

;;; Map a function to each row extracted from SQLite
;;;
(defun sqlite-encoding (db)
  "Return a BABEL suitable encoding for the SQLite db handle."
  (let ((encoding-string (sqlite:execute-single db "pragma encoding;")))
    (cond ((string-equal encoding-string "UTF-8")    :utf-8)
          ((string-equal encoding-string "UTF-16")   :utf-16)
          ((string-equal encoding-string "UTF-16le") :utf-16le)
          ((string-equal encoding-string "UTF-16be") :utf-16be))))

(declaim (inline parse-value))

(defun parse-value (value sqlite-type pgsql-type &key (encoding :utf-8))
  "Parse value given by SQLite to match what PostgreSQL is expecting.
   In some cases SQLite will give text output for a blob column (it's
   base64) and at times will output binary data for text (utf-8 byte
   vector)."
  (cond ((and (string-equal "text" pgsql-type)
              (eq :blob sqlite-type)
              (not (stringp value)))
         ;; we expected a properly encoded string and received bytes instead
         (babel:octets-to-string value :encoding encoding))

        ((and (string-equal "bytea" pgsql-type)
              (stringp value))
         ;; we expected bytes and got a string instead, must be base64 encoded
         (base64:base64-string-to-usb8-array value))

        ;; default case, just use what's been given to us
        (t value)))

(defmethod map-rows ((sqlite copy-sqlite) &key process-row-fn)
  "Extract SQLite data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row"
  (let ((sql      (format nil "SELECT * FROM ~a" (source sqlite)))
        (pgtypes  (map 'vector #'cast-sqlite-column-definition-to-pgsql
                       (fields sqlite))))
    (with-connection (*sqlite-db* (source-db sqlite))
      (let* ((db (conn-handle *sqlite-db*))
             (encoding (sqlite-encoding db)))
        (handler-case
            (loop
               with statement = (sqlite:prepare-statement db sql)
               with len = (loop :for name
                             :in (sqlite:statement-column-names statement)
                             :count name)
               while (sqlite:step-statement statement)
               for row = (let ((v (make-array len)))
                           (loop :for x :below len
                              :for raw := (sqlite:statement-column-value statement x)
                              :for ptype := (aref pgtypes x)
                              :for stype := (sqlite-ffi:sqlite3-column-type
                                             (sqlite::handle statement)
                                             x)
                              :for val := (parse-value raw stype ptype
                                                       :encoding encoding)
                              :do (setf (aref v x) val))
                           v)
               counting t into rows
               do (progn
                    (pgstate-incf *state* (target sqlite) :read 1)
                    (funcall process-row-fn row))
               finally
                 (sqlite:finalize-statement statement)
                 (return rows))
          (condition (e)
            (log-message :error "~a" e)
            (pgstate-incf *state* (target sqlite) :errs 1)))))))


(defmethod copy-to-queue ((sqlite copy-sqlite) queue)
  "Copy data from SQLite table TABLE-NAME within connection DB into queue DATAQ"
  (map-push-queue sqlite queue))

(defmethod copy-from ((sqlite copy-sqlite)
                      &key (kernel nil k-s-p) truncate disable-triggers)
  "Stream the contents from a SQLite database table down to PostgreSQL."
  (let* ((summary     (null *state*))
	 (*state*     (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel* (or kernel (make-kernel 2)))
	 (channel     (lp:make-channel))
	 (queue       (lq:make-queue :fixed-capacity *concurrent-batches*)))

    (with-stats-collection ((target sqlite)
                            :dbname (db-name (target-db sqlite))
                            :state *state*
                            :summary summary)
      (lp:task-handler-bind ((error #'lp:invoke-transfer-error))
        (log-message :notice "COPY ~a" (target sqlite))
        ;; read data from SQLite
        (lp:submit-task channel #'copy-to-queue sqlite queue)

        ;; and start another task to push that data from the queue to PostgreSQL
        (lp:submit-task channel
                        #'pgloader.pgsql:copy-from-queue
                        (target-db sqlite) (target sqlite) queue
                        :truncate truncate
                        :disable-triggers disable-triggers)

        ;; now wait until both the tasks are over
        (loop for tasks below 2 do (lp:receive-result channel)
           finally
             (log-message :info "COPY ~a done." (target sqlite))
             (unless k-s-p (lp:end-kernel)))))))

(defun fetch-sqlite-metadata (sqlite
                              &key
                                state
                                including
                                excluding)
  "SQLite introspection to prepare the migration."
  (let (all-columns all-indexes)
    (with-stats-collection ("fetch meta data"
                            :use-result-as-rows t
                            :use-result-as-read t
                            :state state)
      (with-connection (conn (source-db sqlite))
        (let ((*sqlite-db* (conn-handle conn)))
          (setf all-columns   (filter-column-list (list-all-columns *sqlite-db*)
                                                  :including including
                                                  :excluding excluding)

                all-indexes   (filter-column-list (list-all-indexes *sqlite-db*)
                                                  :including including
                                                  :excluding excluding)))

        ;; return how many objects we're going to deal with in total
        ;; for stats collection
        (+ (length all-columns) (length all-indexes))))

    ;; now return a plist to the caller
    (list :all-columns all-columns
          :all-indexes all-indexes)))

(defmethod copy-database ((sqlite copy-sqlite)
			  &key
			    state-before
			    data-only
			    schema-only
			    (truncate         nil)
			    (disable-triggers nil)
			    (create-tables    t)
			    (include-drop     t)
			    (create-indexes   t)
			    (reset-sequences  t)
                            only-tables
			    including
			    excluding
                            (encoding :utf-8))
  "Stream the given SQLite database down to PostgreSQL."
  (declare (ignore only-tables))
  (let* ((summary       (null *state*))
	 (*state*       (or *state* (make-pgstate)))
	 (state-before  (or state-before (make-pgstate)))
	 (idx-state     (make-pgstate))
	 (seq-state     (make-pgstate))
         (cffi:*default-foreign-encoding* encoding)
         (copy-kernel   (make-kernel 2))
         idx-kernel idx-channel)

    (destructuring-bind (&key all-columns all-indexes pkeys)
        (fetch-sqlite-metadata sqlite
                               :state state-before
                               :including including
                               :excluding excluding)

      (let ((max-indexes
             (loop for (table . indexes) in all-indexes
                maximizing (length indexes))))

        (setf idx-kernel  (when (and max-indexes (< 0 max-indexes))
                            (make-kernel max-indexes)))

        (setf idx-channel (when idx-kernel
                            (let ((lp:*kernel* idx-kernel))
                              (lp:make-channel)))))

      ;; if asked, first drop/create the tables on the PostgreSQL side
      (handler-case
          (cond ((and (or create-tables schema-only) (not data-only))
                 (log-message :notice "~:[~;DROP then ~]CREATE TABLES" include-drop)
                 (with-stats-collection ("create, truncate"
                                         :state state-before
                                         :summary summary)
                   (with-pgsql-transaction (:pgconn (target-db sqlite))
                     (create-tables all-columns :include-drop include-drop))))

                (truncate
                 (truncate-tables (target-db sqlite) (mapcar #'car all-columns))))

        (cl-postgres:database-error (e)
          (declare (ignore e))          ; a log has already been printed
          (log-message :fatal "Failed to create the schema, see above.")
          (return-from copy-database)))

      (loop
         for (table-name . columns) in all-columns
         do
           (let ((table-source
                  (make-instance 'copy-sqlite
                                 :source-db  (source-db sqlite)
                                 :target-db  (target-db sqlite)
                                 :source     table-name
                                 :target     (apply-identifier-case table-name)
                                 :fields     columns)))
             ;; first COPY the data from SQLite to PostgreSQL, using copy-kernel
             (unless schema-only
               (copy-from table-source
                          :kernel copy-kernel
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
               (let* ((indexes
                       (cdr (assoc table-name all-indexes :test #'string=))))
                 (alexandria:appendf
                  pkeys
                  (create-indexes-in-kernel (target-db sqlite) indexes
                                            idx-kernel idx-channel
                                            :state idx-state))))))

      ;; now end the kernels
      (let ((lp:*kernel* copy-kernel))  (lp:end-kernel))
      (let ((lp:*kernel* idx-kernel))
        ;; wait until the indexes are done being built...
        ;; don't forget accounting for that waiting time.
        (when (and create-indexes (not data-only))
          (with-stats-collection ("index build completion" :state *state*)
            (loop for idx in all-indexes do (lp:receive-result idx-channel))))
        (lp:end-kernel))

      ;; don't forget to reset sequences, but only when we did actually import
      ;; the data.
      (when reset-sequences
        (reset-sequences (mapcar #'car all-columns)
                         :pgconn (target-db sqlite)
                         :state seq-state))

      ;; and report the total time spent on the operation
      (report-full-summary "Total streaming time" *state*
                           :before state-before
                           :finally seq-state
                           :parallel idx-state))))


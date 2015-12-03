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
  (let ((transforms (and (slot-boundp source 'transforms)
                         (slot-value  source 'transforms))))
    (when (and (slot-boundp source 'fields) (slot-value source 'fields))
      (loop :for field :in (slot-value source 'fields)
         :for (column fn) := (multiple-value-bind (column fn)
                                 (cast-mysql-column-definition-to-pgsql field)
                               (list column fn))
         :collect column :into columns
         :collect fn :into fns
         :finally (progn (setf (slot-value source 'columns) columns)
                         (unless transforms
                           (setf (slot-value source 'transforms) fns)))))))


;;;
;;; Implement the specific methods
;;;
(defmethod map-rows ((mysql copy-mysql) &key process-row-fn)
  "Extract MySQL data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (let ((table-name             (source mysql))
        (qmynd:*mysql-encoding*
         (when (encoding mysql)
           #+sbcl (encoding mysql)
           #+ccl  (ccl:external-format-character-encoding (encoding mysql)))))

    (with-connection (*connection* (source-db mysql))
      (when qmynd:*mysql-encoding*
        (log-message :notice "Force encoding to ~a for ~a"
                     qmynd:*mysql-encoding* table-name))
      (let* ((cols (get-column-list (db-name (source-db mysql)) table-name))
             (sql  (format nil "SELECT ~{~a~^, ~} FROM `~a`;" cols table-name)))
        (handler-bind
            ;; avoid trying to fetch the character at end-of-input position...
            ((babel-encodings:end-of-input-in-character
              #'(lambda (c)
                  (update-stats :data (target mysql) :errs 1)
                  (log-message :error "~a" c)
                  (invoke-restart 'qmynd-impl::use-nil)))
             (babel-encodings:character-decoding-error
              #'(lambda (c)
                  (update-stats :data (target mysql) :errs 1)
                  (let ((encoding (babel-encodings:character-coding-error-encoding c))
                        (position (babel-encodings:character-coding-error-position c))
                        (character
                         (aref (babel-encodings:character-coding-error-buffer c)
                               (babel-encodings:character-coding-error-position c))))
                    (log-message :error
                                 "~a: Illegal ~a character starting at position ~a: ~a."
                                 table-name encoding position character))
                  (invoke-restart 'qmynd-impl::use-nil))))
          (mysql-query sql :row-fn process-row-fn :result-type 'vector))))))



(defmethod copy-column-list ((mysql copy-mysql))
  "We are sending the data in the MySQL columns ordering here."
  (mapcar #'apply-identifier-case (mapcar #'mysql-column-name (fields mysql))))


;;;
;;; Prepare the PostgreSQL database before streaming the data into it.
;;;
(defun prepare-pgsql-database (pgconn
                               all-columns all-indexes all-fkeys
                               materialize-views view-columns
                               &key
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

  (with-stats-collection ("create, drop" :use-result-as-rows t :section :pre)
    (with-pgsql-transaction (:pgconn pgconn)
      ;; we need to first drop the Foreign Key Constraints, so that we
      ;; can DROP TABLE when asked
      (when (and foreign-keys include-drop)
        (drop-pgsql-fkeys all-fkeys))

      ;; now drop then create tables and types, etc
      (prog1
          (create-tables all-columns :include-drop include-drop)

        ;; MySQL allows the same index name being used against several
        ;; tables, so we add the PostgreSQL table OID in the index name,
        ;; to differenciate. Set the table oids now.
        (set-table-oids all-indexes)

        ;; We might have to MATERIALIZE VIEWS
        (when materialize-views
          (create-tables view-columns :include-drop include-drop))))))

(defun complete-pgsql-database (pgconn all-columns all-fkeys pkeys
                                table-comments column-comments
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
    (reset-sequences (mapcar #'car all-columns) :pgconn pgconn))

  (with-pgsql-connection (pgconn)
    ;;
    ;; Turn UNIQUE indexes into PRIMARY KEYS now
    ;;
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
      (loop :for (table-name . fkeys) :in all-fkeys
         :do (loop :for fkey :in fkeys
                :for sql := (format-pgsql-create-fkey fkey)
                :do (progn
                      (log-message :notice "~a;" sql)
                      (pgsql-execute-with-timing :post "Foreign Keys" sql)))))

    ;;
    ;; And now, comments on tables and columns.
    ;;
    (log-message :notice "Comments")
    (let* ((quote
            ;; just something improbably found in a table comment, to use as
            ;; dollar quoting, and generated at random at that.
            ;;
            ;; because somehow it appears impossible here to benefit from
            ;; the usual SQL injection protection offered by the Extended
            ;; Query Protocol from PostgreSQL.
            (concatenate 'string
                         (map 'string #'code-char
                              (loop :repeat 5
                                 :collect (+ (random 26) (char-code #\A))))
                         "_"
                         (map 'string #'code-char
                              (loop :repeat 5
                                 :collect (+ (random 26) (char-code #\A)))))))
      (loop :for (table-name comment) :in table-comments
         :for sql := (format nil "comment on table ~a is $~a$~a$~a$"
                             (apply-identifier-case table-name)
                             quote comment quote)
         :do (progn
               (log-message :log "~a" sql)
               (pgsql-execute-with-timing :post "Comments" sql)))

      (loop :for (table-name column-name comment) :in column-comments
         :for sql := (format nil "comment on column ~a.~a is $~a$~a$~a$"
                             (apply-identifier-case table-name)
                             (apply-identifier-case column-name)
                             quote comment quote)
         :do (progn
               (log-message :notice "~a;" sql)
               (pgsql-execute-with-timing :post "Comments" sql))))))

(defun fetch-mysql-metadata (mysql
                             &key
                               materialize-views
                               only-tables
                               (create-indexes   t)
                               (foreign-keys     t)
                               including
                               excluding)
  "MySQL introspection to prepare the migration."
  (let ((view-names    (unless (eq :all materialize-views)
                         (mapcar #'car materialize-views)))
        view-columns all-columns all-fkeys all-indexes
        table-comments column-comments)
   (with-stats-collection ("fetch meta data"
                           :use-result-as-rows t
                           :use-result-as-read t
                           :section :pre)
     (with-connection (*connection* (source-db mysql))
       ;; If asked to MATERIALIZE VIEWS, now is the time to create them in
       ;; MySQL, when given definitions rather than existing view names.
       (when (and materialize-views (not (eq :all materialize-views)))
         (create-my-views materialize-views))

       (setf all-columns   (list-all-columns :only-tables only-tables
                                             :including including
                                             :excluding excluding)

             table-comments (list-table-comments :only-tables only-tables
                                                 :including including
                                                 :excluding excluding)

             column-comments (list-columns-comments :only-tables only-tables
                                                    :including including
                                                    :excluding excluding)

             view-columns  (cond (view-names
                                  (list-all-columns :only-tables view-names
                                                    :table-type :view))

                                 ((eq :all materialize-views)
                                  (list-all-columns :table-type :view))))

       (when foreign-keys
         (setf all-fkeys     (list-all-fkeys :only-tables only-tables
                                             :including including
                                             :excluding excluding)))

       (when create-indexes
         (setf all-indexes   (list-all-indexes :only-tables only-tables
                                               :including including
                                               :excluding excluding)))

       ;; return how many objects we're going to deal with in total
       ;; for stats collection
       (+ (length all-columns) (length all-fkeys)
          (length all-indexes) (length view-columns))))

   (log-message :notice
                "MySQL metadata fetched: found ~d tables with ~d indexes total."
                (length all-columns) (length all-indexes))

   ;; now return a plist to the caller
   (list :all-columns all-columns
         :table-comments table-comments
         :column-comments column-comments
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
			    (truncate         nil)
			    (disable-triggers nil)
			    (data-only        nil)
			    (schema-only      nil)
			    (create-tables    t)
			    (include-drop     t)
			    (create-indexes   t)
                            (index-names      :uniquify)
			    (reset-sequences  t)
			    (foreign-keys     t)
			    only-tables
			    including
			    excluding
                            decoding-as
			    materialize-views)
  "Export MySQL data and Import it into PostgreSQL"
  (let* ((copy-kernel  (make-kernel 8))
         (copy-channel (let ((lp:*kernel* copy-kernel)) (lp:make-channel)))
         (table-count  0)
         idx-kernel idx-channel)

    (destructuring-bind (&key view-columns all-columns
                              table-comments column-comments
                              all-fkeys all-indexes pkeys)
        ;; to prepare the run, we need to fetch MySQL meta-data
        (fetch-mysql-metadata mysql
                              :materialize-views materialize-views
                              :only-tables only-tables
                              :create-indexes create-indexes
                              :foreign-keys foreign-keys
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
                 (prepare-pgsql-database (target-db mysql)
                                         all-columns
                                         all-indexes
                                         all-fkeys
                                         materialize-views
                                         view-columns
                                         :foreign-keys foreign-keys
                                         :include-drop include-drop))
                (t
                 (when truncate
                   (truncate-tables (target-db mysql) (mapcar #'car all-columns)))))
        ;;
        ;; In case some error happens in the preparatory transaction, we
        ;; need to stop now and refrain from trying to load the data into
        ;; an incomplete schema.
        ;;
        (cl-postgres:database-error (e)
          (declare (ignore e))		; a log has already been printed
          (log-message :fatal "Failed to create the schema, see above.")

          ;; we did already create our Views in the MySQL database, so clean
          ;; that up now.
          (when materialize-views
            (with-connection (*connection* (source-db mysql))
              (drop-my-views materialize-views)))

          (return-from copy-database)))

      (loop
         :for (table-name . columns) :in (append all-columns view-columns)

         :unless columns
         :do (log-message :error "Table ~s not found, skipping." table-name)

         :when columns
         :do
           (let* ((encoding
                   ;; force the data encoding when asked to
                   (when decoding-as
                     (loop :for (encoding . filters) :in decoding-as
                        :when (apply-decoding-as-filters table-name filters)
                        :return encoding)))

                  (table-source
                   (make-instance 'copy-mysql
                                  :source-db  (clone-connection (source-db mysql))
                                  :target-db  (clone-connection (target-db mysql))
                                  :source     table-name
                                  :target     (apply-identifier-case table-name)
                                  :fields     columns
                                  :encoding   encoding)))

             (log-message :debug "TARGET: ~a" (target table-source))

             ;; first COPY the data from MySQL to PostgreSQL, using copy-kernel
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
               (let* ((indexes
                       (cdr (assoc table-name all-indexes :test #'string=)))
                      (*preserve-index-names* (eq :preserve index-names)))
                 (alexandria:appendf
                  pkeys
                  (create-indexes-in-kernel (target-db mysql)
                                            indexes idx-kernel idx-channel))))))

      ;; now end the kernels
      (let ((lp:*kernel* copy-kernel))
        (with-stats-collection ("COPY Threads Completion" :section :post
                                                          :use-result-as-read t
                                                          :use-result-as-rows t)
            (let ((workers-count (* 4 table-count)))
              (loop :for tasks :below workers-count
                 :do (destructuring-bind (task table-name seconds)
                         (lp:receive-result copy-channel)
                       (log-message :debug "Finished processing ~a for ~s ~50T~6$s"
                                    task table-name seconds)
                       (when (eq :writer task)
                         (update-stats :data table-name :secs seconds))))
              (prog1
                  workers-count
                (lp:end-kernel)))))

      (let ((lp:*kernel* idx-kernel))
        ;; wait until the indexes are done being built...
        ;; don't forget accounting for that waiting time.
        (when (and create-indexes (not data-only))
          (with-stats-collection ("Index Build Completion" :section :post
                                                           :use-result-as-read t
                                                           :use-result-as-rows t)
              (let ((nb-indexes
                     (reduce #'+ all-indexes :key (lambda (entry)
                                                    (length (cdr entry))))))
                (log-message :debug "Waiting for ~a index completion" nb-indexes)
                (loop :for count :below nb-indexes
                   :do (lp:receive-result idx-channel))
                nb-indexes)))
        (lp:end-kernel))

      ;;
      ;; If we created some views for this run, now is the time to DROP'em
      ;;
      (when materialize-views
        (with-connection (*connection* (source-db mysql))
          (drop-my-views materialize-views)))

      ;;
      ;; Complete the PostgreSQL database before handing over.
      ;;
      (complete-pgsql-database (clone-connection (target-db mysql))
                               all-columns all-fkeys pkeys
                               table-comments column-comments
                               :data-only data-only
                               :foreign-keys foreign-keys
                               :reset-sequences reset-sequences))))

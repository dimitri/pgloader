;;;
;;; Tools to handle PostgreSQL tables and indexes creations
;;;
(in-package #:pgloader.pgsql)


;;;
;;; UUID extension dependency tracking
;;;
(defun catalog-add-uuid-extension (catalog pgversion)
  "Scan CATALOG for columns with :generate-uuid defaults and ensure the
   correct extension and function are used based on PGVERSION.

   PostgreSQL 13+ ships gen_random_uuid() in core; older versions need the
   uuid-ossp extension and uuid_generate_v4().  This function rewrites
   :generate-uuid defaults in place and adds uuid-ossp to the extension list
   when needed."
  (let* ((major (if (stringp pgversion)
                    (parse-integer pgversion :junk-allowed t)
                    pgversion))
         (has-builtin-uuid (and major (>= major 13))))
    (loop :for schema :in (catalog-schema-list catalog)
       :do (loop :for table :in (schema-table-list schema)
              :do (loop :for column :in (table-column-list table)
                     :when (eq :generate-uuid (column-default column))
                     :do (if has-builtin-uuid
                             ;; PG 13+: gen_random_uuid() is built-in
                             (setf (column-default column) :generate-uuid-builtin)
                             ;; PG < 13: need uuid-ossp extension
                             (maybe-add-extension schema "uuid-ossp")))))))

;;;
;;; Table schema support
;;;
(defun type-names-existing-in-schema (schema-name names)
  "Return the subset of NAMES already taken in SCHEMA-NAME as a list of
   strings (one query, ANY($2) array).  Every table registers an implicit
   composite type in pg_type, so this covers both tables and explicit types."
  (pomo:query
   "SELECT t.typname
      FROM pg_catalog.pg_type t
      JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
     WHERE n.nspname = $1
       AND t.typname = ANY($2)"
   schema-name (coerce names 'vector)
   :column))

(defun resolve-enum-type-name (schema-name base-name)
  "Return the first candidate name for an ENUM/SET type that does not
   already exist in SCHEMA-NAME.  BASE-NAME is expected to end in '_t'.
   Alternatives strip that suffix and append '_enum', or prepend 'enum_'.
   All three candidates are checked in a single query.
   Signals an error when all candidates conflict."
  (let* ((stem (if (and (> (length base-name) 2)
                        (string= "_t" (subseq base-name (- (length base-name) 2))))
                   (subseq base-name 0 (- (length base-name) 2))
                   base-name))
         (candidates (list base-name
                           (format nil "~a_enum" stem)
                           (format nil "enum_~a" stem)))
         (taken (type-names-existing-in-schema schema-name candidates)))
    (or (find-if (lambda (name) (not (member name taken :test #'string=))) candidates)
        (error "~@<Could not find a non-conflicting PostgreSQL type name ~
                for enum ~s in schema ~s; tried: ~{~s~^, ~}~:>"
               base-name schema-name candidates))))

(defun create-sqltypes (catalog
                        &key
                          if-not-exists
                          include-drop
                          (client-min-messages :notice))
  "Create the needed data types for given CATALOG."
  (let ((sqltype-list (sqltype-list catalog)))
    (loop :for sqltype :in sqltype-list
       :do (setf (sqltype-name sqltype)
                 (resolve-enum-type-name
                  (ensure-unquoted (schema-name (sqltype-schema sqltype)))
                  (sqltype-name sqltype)))
       :when include-drop
       :count t
       :do (pgsql-execute (format-drop-sql sqltype :cascade t :if-exists t)
                          :client-min-messages client-min-messages)
       :do (pgsql-execute
            (format-create-sql sqltype :if-not-exists if-not-exists)
            :client-min-messages client-min-messages))))

(defun create-table-sql-list (table-list
                              &key
                                if-not-exists
                                include-drop)
  "Return the list of CREATE TABLE statements to run against PostgreSQL."
  (loop :for table :in table-list

     :when include-drop
     :collect (format-drop-sql table :cascade t :if-exists t)

     :collect (format-create-sql table :if-not-exists if-not-exists)))

(defun create-table-list (table-list
                          &key
                            if-not-exists
                            include-drop
                            (client-min-messages :notice))
  "Create all tables in database dbname in PostgreSQL."
  (loop
     :for sql :in (create-table-sql-list table-list
                                         :if-not-exists if-not-exists
                                         :include-drop include-drop)
     :count (not (null sql)) :into nb-tables
     :when sql
     :do (pgsql-execute sql :client-min-messages client-min-messages)
     :finally (return nb-tables)))

(defun create-sequences (catalog
                         &key
                           include-drop
                           (client-min-messages :notice))
  "Create all sequences from CATALOG's :sequences list.
   Each entry is a plist with :schema-name :sequence-name :start-value
   :increment-by :minimum-value :maximum-value :current-value :is-cycling
   :cache-size (as returned by fetch-sequences).
   The start is set to current+increment so the first nextval() returns
   the next un-issued value."
  (loop :for seq :in (catalog-sequences catalog)
     :for schema-name   := (getf seq :schema-name)
     :for pg-schema     := (if (string= (string-downcase schema-name) "dbo")
                               "public"
                               schema-name)
     :for seq-name      := (getf seq :sequence-name)
     :for qname         := (format nil "\"~a\".\"~a\"" pg-schema seq-name)
     :for start         := (getf seq :start-value)
     :for increment     := (getf seq :increment-by)
     :for minimum       := (getf seq :minimum-value)
     :for maximum       := (getf seq :maximum-value)
     :for current       := (getf seq :current-value)
     :for is-cycling    := (getf seq :is-cycling)
     :for cache         := (getf seq :cache-size)
     :for next-val      := (if current (+ current increment) start)
     :do
     (when include-drop
       (pgsql-execute (format nil "DROP SEQUENCE IF EXISTS ~a CASCADE;" qname)
                      :client-min-messages client-min-messages))
     (pgsql-execute
      (format nil "CREATE SEQUENCE IF NOT EXISTS ~a AS bigint~
                   ~% START WITH ~a INCREMENT BY ~a~
                   ~% MINVALUE ~a MAXVALUE ~a~
                   ~%~:[NO CYCLE~;CYCLE~]~:[~; CACHE ~a~];"
              qname next-val increment minimum maximum
              is-cycling
              (and cache (> cache 0)) cache)
      :client-min-messages client-min-messages)))

(defun create-schemas (catalog
                       &key
                         include-drop
                         (client-min-messages :notice))
  "Create all schemas from the given database CATALOG."
  (let ((schema-list (list-schemas)))
    (when include-drop
      ;; if asked, first DROP the schema CASCADE.
      (loop :for schema :in (catalog-schema-list catalog)
         :for schema-name := (schema-name schema)
         :when (and schema-name
                    (member (ensure-unquoted schema-name)
                            schema-list
                            :test #'string=))
         :do (let ((sql (format nil "DROP SCHEMA ~a CASCADE;" schema-name)))
               (pgsql-execute sql :client-min-messages client-min-messages))))

    ;; now create the schemas (again?)
    (loop :for schema :in (catalog-schema-list catalog)
       :for schema-name := (schema-name schema)
       :when (and schema-name
                  (or include-drop
                      (not (member (ensure-unquoted schema-name)
                                   schema-list
                                   :test #'string=))))
       :do (let ((sql (format nil "CREATE SCHEMA ~a;" (schema-name schema))))
             (pgsql-execute sql :client-min-messages client-min-messages)))))

(defun add-to-search-path (catalog
                           &key
                             label
                             (section :post)
                             (log-level :notice)
                             (client-min-messages :notice))
  "Add catalog schemas in the database search_path."
  (let* ((dbname      (get-current-database))
         (search-path (list-search-path))
         (missing-schemas
          (loop :for schema :in (catalog-schema-list catalog)
             :for schema-name := (schema-name schema)
             :when (and (schema-in-search-path schema)
                        (not (member schema-name search-path :test #'string=)))
             :collect schema-name)))
    (when missing-schemas
      (let ((sql (format nil
                         "ALTER DATABASE ~s SET search_path TO ~{~a~^, ~};"
                         dbname
                         (append search-path missing-schemas))))
        (pgsql-execute-with-timing section
                                   label
                                   sql
                                   :log-level log-level
                                   :client-min-messages client-min-messages)))))

(defun list-installed-extensions ()
  "Return a list of extension names currently installed in the database."
  (mapcar #'first
          (pomo:query "select extname from pg_extension")))

(defun create-extensions (catalog
                          &key
                            if-not-exists
                            include-drop
                            (client-min-messages :notice))
  "Create all extensions from the given database CATALOG.

   Extensions that are already installed are skipped (no superuser needed for
   that case).  When an extension is missing and CREATE EXTENSION fails — most
   commonly because the connected role lacks superuser / CREATE privileges —
   a clear error is logged explaining what the user must do manually."
  (let ((installed (list-installed-extensions)))
    (loop :for extension :in (extension-list catalog)
       :for ext-name := (extension-name extension)
       :do (cond
             ;; DROP requested: always attempt it
             (include-drop
              (pgsql-execute (format-drop-sql extension :if-exists t :cascade t)
                             :client-min-messages client-min-messages))
             (t nil))
       :do (cond
             ;; Already installed — nothing to do
             ((member ext-name installed :test #'string=)
              (log-message :info "Extension ~s is already installed, skipping." ext-name))

             ;; Try to create it; catch privilege errors and emit a clear message
             (t
              (let ((create-sql (format-create-sql extension
                                                   :if-not-exists if-not-exists)))
                (handler-case
                    (pgsql-execute create-sql :client-min-messages client-min-messages)
                  (cl-postgres:database-error (e)
                    (log-message :error
                                 "Could not create extension ~s: ~a"
                                 ext-name e)
                    (log-message :error
                                 "The pgloader role may lack CREATE/superuser privileges. ~
                                  Ask a database superuser to run:~%~
                                    CREATE EXTENSION IF NOT EXISTS ~s;~%~
                                  then retry pgloader."
                                 ext-name ext-name)
                    (error e)))))))))

(defun create-tables (catalog
                      &key
			if-not-exists
			include-drop
			(client-min-messages :notice))
  "Create all tables from the given database CATALOG."
  (create-table-list (table-list catalog)
                     :if-not-exists if-not-exists
                     :include-drop include-drop
                     :client-min-messages client-min-messages))

(defun create-views (catalog
                     &key
                       if-not-exists
                       include-drop
                       (client-min-messages :notice))
  "Create all tables from the given database CATALOG."
  (create-table-list (view-list catalog)
                     :if-not-exists if-not-exists
                     :include-drop include-drop
                     :client-min-messages client-min-messages))

(defun create-triggers (catalog
                        &key
                          label
                          (section :post)
                          (client-min-messages :notice))
  "Create the catalog objects that come after the data has been loaded."
  (let ((sql-list
         (loop :for table :in (table-list catalog)
            :do (process-triggers table)
            :when (table-trigger-list table)
            :append (loop :for trigger :in (table-trigger-list table)
                       :collect (format-create-sql (trigger-procedure trigger))
                       :collect (format-create-sql trigger)))))
    (pgsql-execute-with-timing section label sql-list
                               :log-level :sql
                               :client-min-messages client-min-messages)))


;;;
;;; DDL Utilities: TRUNCATE, ENABLE/DISABLE triggers
;;;

(defun truncate-tables (catalog-or-table)
  "Truncate given TABLE-NAME in database DBNAME. A PostgreSQL connection
   must already be active when calling that function."
  (let* ((target-list (mapcar #'format-table-name
                              (etypecase catalog-or-table
                                (catalog (table-list catalog-or-table))
                                (schema  (table-list catalog-or-table))
                                (table   (list catalog-or-table)))))
         (sql
          (when target-list
            (format nil "TRUNCATE ~{~a~^,~};" target-list))))
    (if target-list
        (progn
          (pgsql-execute sql)
          ;; return how many tables we just TRUNCATEd
          (length target-list))
        0)))

(defun disable-triggers (table-name)
  "Disable triggers on TABLE-NAME. Needs to be called with a PostgreSQL
   connection already opened."
  (let ((sql (format nil "ALTER TABLE ~a DISABLE TRIGGER ALL;"
                     (apply-identifier-case table-name))))
    (pgsql-execute sql)))

(defun enable-triggers (table-name)
  "Disable triggers on TABLE-NAME. Needs to be called with a PostgreSQL
   connection already opened."
  (let ((sql (format nil "ALTER TABLE ~a ENABLE TRIGGER ALL;"
                     (apply-identifier-case table-name))))
    (pgsql-execute sql)))

(defmacro with-disabled-triggers ((table-name &key disable-triggers)
                                  &body forms)
  "Run FORMS with PostgreSQL triggers disabled for TABLE-NAME if
   DISABLE-TRIGGERS is T A PostgreSQL connection must be opened already
   where this macro is used."
  `(if ,disable-triggers
       (progn
         (disable-triggers ,table-name)
         (unwind-protect
              (progn ,@forms)
           (enable-triggers ,table-name)))
       (progn ,@forms)))


;;;
;;; API for Foreign Keys
;;;
(defun drop-pgsql-fkeys (catalog &key (cascade t) (log-level :notice))
  "Drop all Foreign Key Definitions given, to prepare for a clean run."
  (let ((fk-sql-list
         (loop :for table :in (table-list catalog)
            :append (loop :for fkey :in (table-fkey-list table)
                       :collect (format-drop-sql fkey
                                                 :cascade cascade
                                                 :if-exists t))
            ;; also DROP the foreign keys that depend on the indexes we
            ;; want to DROP
            :append (loop :for index :in (table-index-list table)
                       :append (loop :for fkey :in (index-fk-deps index)
                                  :collect (format-drop-sql fkey
                                                            :cascade t
                                                            :if-exists t))))))
    (pgsql-execute fk-sql-list :log-level log-level)))

(defun create-pgsql-fkeys (catalog &key (section :post) label log-level)
  "Actually create the Foreign Key References that where declared in the
   MySQL database"
  (let ((fk-sql-list
         (loop :for table :in (table-list catalog)
            :append (loop :for fkey :in (table-fkey-list table)
                       ;; we might have loaded fkeys referencing tables that
                       ;; have not been included in (or have been excluded
                       ;; from) the load
                       :unless (and (fkey-table fkey)
                                    (fkey-foreign-table fkey))
                       :do (log-message :debug "Skipping foreign key ~a" fkey)
                       :when (and (fkey-table fkey)
                                  (fkey-foreign-table fkey))
                       :collect (format-create-sql fkey))
            :append (loop :for index :in (table-index-list table)
                       :do (loop :for fkey :in (index-fk-deps index)
                              :for sql := (format-create-sql fkey)
                              :do (log-message :debug "EXTRA FK DEPS! ~a" sql)
                              :collect sql)))))
    ;; and now execute our list
    (pgsql-execute-with-timing section label fk-sql-list :log-level log-level)))



;;;
;;; CHECK constraints
;;;
(defun create-pgsql-check-constraints (catalog &key (section :post) label)
  "Generate and execute ALTER TABLE … ADD CONSTRAINT … CHECK (…) for every
   table that has check constraints recorded in its check-constraint-list."
  (let ((sql-list
         (loop :for table :in (table-list catalog)
            :append
            (loop :for (name . clause) :in (table-check-constraint-list table)
               :for table-sql := (format nil
                                         "ALTER TABLE ~a ADD CONSTRAINT ~a CHECK (~a);"
                                         (format-table-name table)
                                         (apply-identifier-case name)
                                         clause)
               :collect table-sql))))
    (when sql-list
      (pgsql-execute-with-timing section label sql-list))))


;;;
;;; Parallel index building.
;;;
(defun create-indexes-in-kernel (pgconn table kernel channel
				 &key (label "Create Indexes"))
  "Create indexes for given table in dbname, using given lparallel KERNEL
   and CHANNEL so that the index build happen in concurrently with the data
   copying."
  (let* ((lp:*kernel* kernel))
    (loop
       :for index :in (table-index-list table)
       :for pkey := (multiple-value-bind (sql pkey)
                        ;; we postpone the pkey upgrade of the index for later.
                        (format-create-sql index)

                      (lp:submit-task channel
                                      #'pgsql-connect-and-execute-with-timing
                                      ;; each thread must have its own connection
                                      (clone-connection pgconn)
                                      :post label sql)

                      ;; return the pkey "upgrade" statement
                      pkey)
       :when pkey
       :collect pkey)))


;;;
;;; Protect from non-unique index names
;;;
(defun set-table-oids (catalog &key (variant :pgdg))
  "MySQL allows using the same index name against separate tables, which
   PostgreSQL forbids. To get unicity in index names without running out of
   characters (we are allowed only 63), we use the table OID instead.

   This function grabs the table OIDs in the PostgreSQL database and update
   the definitions with them."
  (let ((oid-map (list-table-oids catalog :variant variant)))
    (loop :for table :in (table-list catalog)
       :for table-name := (format-table-name table)
       :for table-oid := (gethash table-name oid-map)
       :unless table-oid :do (error "OID not found for ~s." table-name)
       :count t
       :do (setf (table-oid table) table-oid))))


;;;
;;; Drop indexes before loading
;;;
(defun drop-indexes (table-or-catalog &key cascade (log-level :notice))
  "Drop indexes in PGSQL-INDEX-LIST. A PostgreSQL connection must already be
   active when calling that function."
  (let ((sql-index-list
         (loop :for index
            :in (typecase table-or-catalog
                  (table   (table-index-list table-or-catalog))
                  (catalog (loop :for table :in (table-list table-or-catalog)
                              :append (table-index-list table))))
            :collect (format-drop-sql index :cascade cascade :if-exists t))))
    (pgsql-execute sql-index-list :log-level log-level)
    ;; return how many indexes we just DROPed
    (length sql-index-list)))

;;;
;;; Higher level API to care about indexes
;;;
(defun maybe-drop-indexes (catalog &key drop-indexes)
  "Drop the indexes for TABLE-NAME on TARGET PostgreSQL connection, and
   returns a list of indexes to create again. A PostgreSQL connection must
   already be active when calling that function."
  (loop :for table :in (table-list catalog)
     :do
     (let ((indexes (table-index-list table))
           ;; we get the list of indexes from PostgreSQL catalogs, so don't
           ;; question their spelling, just quote them.
           (*identifier-case* :quote))

       (cond ((and indexes (not drop-indexes))
              (log-message :warning
                           "Target table ~s has ~d indexes defined against it."
                           (format-table-name table) (length indexes))
              (log-message :warning
                           "That could impact loading performance badly.")
              (log-message :warning
                           "Consider the option 'drop indexes'."))

             (indexes
              (drop-indexes table))))))

(defun create-indexes-again (target catalog
                             &key
                               max-parallel-create-index
                               (section :post)
                               drop-indexes)
  "Create the indexes that we dropped previously."
  (when (and drop-indexes
             (< 0 (count-indexes catalog)))
    (let* ((*preserve-index-names* t)
           ;; we get the list of indexes from PostgreSQL catalogs, so don't
           ;; question their spelling, just quote them.
           (*identifier-case* :quote)
           (idx-kernel  (make-kernel (or max-parallel-create-index
                                         (count-indexes catalog))))
           (idx-channel (let ((lp:*kernel* idx-kernel))
                          (lp:make-channel))))
      (loop :for table :in (table-list catalog)
         :when (table-index-list table)
         :do
         (let ((pkeys
                (create-indexes-in-kernel target table idx-kernel idx-channel)))

           (with-stats-collection ("Index Build Completion" :section section)
               (loop :repeat (count-indexes table)
                  :do (lp:receive-result idx-channel))
             (lp:end-kernel :wait t))

           ;; turn unique indexes into pkeys now
           (pgsql-connect-and-execute-with-timing target
                                                  section
                                                  "Constraints"
                                                  pkeys))))))


;;;
;;; Sequences
;;;
(defun reset-sequences (target catalog &key (section :post))
  "Reset all sequences created during this MySQL migration."
  (log-message :notice "Reset sequences")
  (with-stats-collection ("Reset Sequences"
                          :use-result-as-read t
                          :use-result-as-rows t
                          :section section)
      (let ((tables  (table-list catalog)))
        (with-pgsql-connection (target)
          (set-session-gucs *pg-settings*)
          (pomo:execute "set client_min_messages to warning;")
          (pomo:execute "listen seqs")

          (when tables
            (pomo:execute
             (format nil "create temp table reloids(oid) as values ~{('~a'::regclass)~^,~}"
                     (mapcar #'format-table-name tables))))

          (handler-case
              (let ((sql (format nil "
DO $$
DECLARE
  n integer := 0;
  r record;
BEGIN
  FOR r in
       SELECT 'select '
               || trim(trailing ')'
                  from replace(pg_get_expr(d.adbin, d.adrelid),
                               'nextval', 'setval'))
               || ', (select greatest(max(' || quote_ident(a.attname) || '), (select seqmin from pg_sequence where seqrelid = ('''
               || pg_get_serial_sequence(quote_ident(nspname) || '.' || quote_ident(relname), a.attname) || ''')::regclass limit 1), 1) from only '
               || quote_ident(nspname) || '.' || quote_ident(relname) || '));' as sql
         FROM pg_class c
              JOIN pg_namespace n on n.oid = c.relnamespace
              JOIN pg_attribute a on a.attrelid = c.oid
              JOIN pg_attrdef d on d.adrelid = a.attrelid
                                 and d.adnum = a.attnum
                                 and a.atthasdef
        WHERE relkind = 'r' and a.attnum > 0
              and pg_get_expr(d.adbin, d.adrelid) ~~ '^nextval'
              and pg_get_serial_sequence(quote_ident(nspname) || '.' || quote_ident(relname), a.attname) is not null
              ~@[and c.oid in (select oid from reloids)~]
  LOOP
    n := n + 1;
    EXECUTE r.sql;
  END LOOP;

  PERFORM pg_notify('seqs', n::text);
END;
$$; " tables)))
                (pomo:execute sql))
            ;; now get the notification signal
            (cl-postgres:postgresql-notification (c)
              (parse-integer (cl-postgres:postgresql-notification-payload c))))))))


;;;
;;; Comments
;;;
(defun comment-on-tables-and-columns (catalog &key label (section :post))
  "Install comments on tables and columns from CATALOG."
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
                               :collect (+ (random 26) (char-code #\A))))))

         (sql-list
          ;; table level comments
          (loop :for table :in (table-list catalog)
             :when (table-comment table)
             :collect (format nil "comment on table ~a is $~a$~a$~a$"
                              (format-table-name table)
                              quote (table-comment table) quote)

             ;; for each table, append column level comments
             :append
             (loop :for column :in (table-column-list table)
                :when (column-comment column)
                :collect (format nil
                                 "comment on column ~a.~a is $~a$~a$~a$"
                                 (format-table-name table)
                                 (column-name column)
                                 quote (column-comment column) quote)))))
    (pgsql-execute-with-timing section label sql-list)))



;;;
;;; Citus Disitribution support
;;;
(defun create-distributed-table (distribute-rules)
  (let ((citus-sql
         (loop :for rule :in distribute-rules
            :collect (format-create-sql rule))))
    (pgsql-execute citus-sql)))

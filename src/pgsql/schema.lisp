;;;
;;; Tools to handle PostgreSQL tables and indexes creations
;;;
(in-package pgloader.pgsql)

;;;
;;; Some parts of the logic here needs to be specialized depending on the
;;; source type, such as SQLite or MySQL. To do so, sources must define
;;; their own column struct and may implement the methods
;;; `format-pgsql-column' and `format-extra-type' on those.
;;;
(defstruct pgsql-column name type-name type-mod nullable default)

(defgeneric format-pgsql-column (col)
  (:documentation
   "Return the PostgreSQL column definition (type, default, not null, ...)"))

(defgeneric format-extra-type (col &key include-drop)
  (:documentation
   "Return a list of PostgreSQL commands to create an extra type for given
    column, or nil of none is required. If no special extra type is ever
    needed, it's allowed not to specialize this generic into a method."))

;; (defmethod format-pgsql-column ((col pgsql-column))
;;   "Return a string representing the PostgreSQL column definition."
;;   (let* ((column-name
;; 	  (apply-identifier-case (pgsql-column-name col)))
;; 	 (type-definition
;; 	  (format nil
;; 		  "~a~@[~a~]~:[~; not null~]~@[ default ~a~]"
;; 		  (pgsql-column-type-name col)
;; 		  (pgsql-column-type-mod col)
;; 		  (pgsql-column-nullable col)
;; 		  (pgsql-column-default col))))
;;     (format nil "~a ~22t ~a" column-name type-definition)))

(defmethod format-extra-type ((col T) &key include-drop)
  "The default `format-extra-type' implementation returns an empty list."
  (declare (ignorable include-drop))
  nil)


;;;
;;; API for Foreign Keys
;;;
(defstruct pgsql-fkey
  name table-name columns foreign-table foreign-columns update-rule delete-rule)

(defgeneric format-pgsql-create-fkey (fkey)
  (:documentation
   "Return the PostgreSQL command to define a Foreign Key Constraint."))

(defgeneric format-pgsql-drop-fkey (fkey &key)
  (:documentation
   "Return the PostgreSQL command to DROP a Foreign Key Constraint."))

(defmethod format-pgsql-create-fkey ((fk pgsql-fkey))
  "Generate the PostgreSQL statement to rebuild a MySQL Foreign Key"
  (format nil
          "ALTER TABLE ~a ADD CONSTRAINT ~a FOREIGN KEY(~{~a~^,~}) REFERENCES ~a(~{~a~^,~})~:[~*~; ON UPDATE ~a~]~:[~*~; ON DELETE ~a~]"
          (pgsql-fkey-table-name fk)
          (pgsql-fkey-name fk)        ; constraint name
          (pgsql-fkey-columns fk)
          (pgsql-fkey-foreign-table fk)
          (pgsql-fkey-foreign-columns fk)
          (pgsql-fkey-update-rule fk)
          (pgsql-fkey-update-rule fk)
          (pgsql-fkey-delete-rule fk)
          (pgsql-fkey-delete-rule fk)))

(defmethod format-pgsql-drop-fkey ((fk pgsql-fkey) &key all-pgsql-fkeys)
  "Generate the PostgreSQL statement to rebuild a MySQL Foreign Key"
  (let* ((constraint-name (apply-identifier-case (pgsql-fkey-name fk)))
	 (table-name      (apply-identifier-case (pgsql-fkey-table-name fk)))
	 (fkeys         (cdr (assoc table-name all-pgsql-fkeys :test #'string=)))
	 (fkey-exists   (member constraint-name fkeys :test #'string=)))
    (when fkey-exists
      ;; we could do that without all-pgsql-fkeys in 9.2 and following with:
      ;; alter table if exists ... drop constraint if exists ...
      (format nil "ALTER TABLE ~a DROP CONSTRAINT ~a" table-name constraint-name))))

(defun drop-pgsql-fkeys (catalog)
  "Drop all Foreign Key Definitions given, to prepare for a clean run."
  (let ((all-pgsql-fkeys (list-tables-and-fkeys)))
    (loop :for table :in (table-list catalog)
       :do
       (loop :for fkey :in (table-fkey-list table)
          :for sql := (format-pgsql-drop-fkey fkey
                                              :all-pgsql-fkeys all-pgsql-fkeys)
          :when sql
          :do
          (log-message :notice "~a;" sql)
          (pgsql-execute sql)))))

(defun create-pgsql-fkeys (catalog
                           &key
                             (section :post)
                             (label "Foreign Keys"))
  "Actually create the Foreign Key References that where declared in the
   MySQL database"
  (with-stats-collection (label :section section :use-result-as-rows t)
      (loop :for table :in (table-list catalog)
         :sum (loop :for fkey :in (table-fkey-list table)
                 :for sql := (format-pgsql-create-fkey fkey)
                 :do (progn             ; for indentation purposes
                       (log-message :notice "~a;" sql)
                       (pgsql-execute-with-timing section label sql))
                 :count t))))


;;;
;;; Table schema rewriting support
;;;
(defun create-table-sql (table &key if-not-exists)
  "Return a PostgreSQL CREATE TABLE statement from given COLS.

   Each element of the COLS list is expected to be of a type handled by the
   `format-pgsql-column' generic function."
  (with-output-to-string (s)
    (format s "CREATE TABLE~:[~; IF NOT EXISTS~] ~a ~%(~%"
            if-not-exists
            (format-table-name table))
    (loop
       :for (col . last?) :on (table-column-list table)
       :for pg-coldef := (format-column col)
       :do (format s "  ~a~:[~;,~]~%" pg-coldef last?))
    (format s ");~%")))

(defun drop-table-if-exists-sql (table)
  "Return the PostgreSQL DROP TABLE IF EXISTS statement for TABLE-NAME."
  (format nil "DROP TABLE IF EXISTS ~a CASCADE;" (format-table-name table)))

(defun create-table-sql-list (table-list
			      &key
				if-not-exists
				include-drop)
  "Return the list of CREATE TABLE statements to run against PostgreSQL."
  (loop
     :for table :in table-list
     :for cols  := (table-column-list table)
     :for fields := (table-field-list table)
     :for extra-types := (loop :for field :in fields
                            :append (format-extra-type
                                     field :include-drop include-drop))

     :when include-drop
     :collect (drop-table-if-exists-sql table)

     :when extra-types :append extra-types

     :collect (create-table-sql table :if-not-exists if-not-exists)))

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
     :do (progn
           (log-message :info "~a" sql)
           (pgsql-execute sql :client-min-messages client-min-messages))
     :finally (return nb-tables)))

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

(defun truncate-tables (pgconn catalog-or-table)
  "Truncate given TABLE-NAME in database DBNAME"
  (with-pgsql-transaction (:pgconn pgconn)
    (let ((sql
           (format nil "TRUNCATE ~{~a~^,~};"
                   (mapcar #'format-table-name
                           (etypecase catalog-or-table
                             (catalog (table-list catalog-or-table))
                             (schema  (table-list catalog-or-table))
                             (table   (list catalog-or-table)))))))
      (log-message :notice "~a" sql)
      (pomo:execute sql))))

(defun disable-triggers (table-name)
  "Disable triggers on TABLE-NAME. Needs to be called with a PostgreSQL
   connection already opened."
  (let ((sql (format nil "ALTER TABLE ~a DISABLE TRIGGER ALL;"
                     (apply-identifier-case table-name))))
    (log-message :info "~a" sql)
    (pomo:execute sql)))

(defun enable-triggers (table-name)
  "Disable triggers on TABLE-NAME. Needs to be called with a PostgreSQL
   connection already opened."
  (let ((sql (format nil "ALTER TABLE ~a ENABLE TRIGGER ALL;"
                     (apply-identifier-case table-name))))
    (log-message :info "~a" sql)
    (pomo:execute sql)))

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
;;; Index support
;;;
(defstruct pgsql-index
  ;; the struct is used both for supporting new index creation from non
  ;; PostgreSQL system and for drop/create indexes when using the 'drop
  ;; indexes' option (in CSV mode and the like)
  name schema table-name table-oid primary unique columns sql conname condef)

(defgeneric format-pgsql-create-index (table index)
  (:documentation
   "Return the PostgreSQL command to define an Index."))

(defgeneric format-pgsql-drop-index (table index)
  (:documentation
   "Return the PostgreSQL command to drop an Index."))

(defmethod format-pgsql-create-index ((table table) (index pgsql-index))
  "Generate the PostgreSQL statement list to rebuild a Foreign Key"
  (let* ((index-name (if (and *preserve-index-names*
                              (not (string-equal "primary" (pgsql-index-name index)))
                              (pgsql-index-table-oid index))
                         (pgsql-index-name index)

                         ;; in the general case, we build our own index name.
                         (format nil "idx_~a_~a"
                                 (pgsql-index-table-oid index)
                                 (pgsql-index-name index))))
	 (index-name (apply-identifier-case index-name)))
    (cond
      ((or (pgsql-index-primary index)
           (and (pgsql-index-condef index) (pgsql-index-unique index)))
       (values
        ;; ensure good concurrency here, don't take the ACCESS EXCLUSIVE
        ;; LOCK on the table before we have the index done already
        (or (pgsql-index-sql index)
            (format nil "CREATE UNIQUE INDEX ~@[~a.~]~a ON ~a (~{~a~^, ~});"
                    (pgsql-index-schema index)
                    index-name
                    (format-table-name table)
                    (pgsql-index-columns index)))
        (format nil
                ;; don't use the index schema name here, PostgreSQL doesn't
                ;; like it, might be implicit from the table's schema
                ;; itself...
                "ALTER TABLE ~a ADD ~a USING INDEX ~a;"
                (format-table-name table)
                (cond ((pgsql-index-primary index) "PRIMARY KEY")
                      ((pgsql-index-unique index) "UNIQUE"))
                index-name)))

      ((pgsql-index-condef index)
       (format nil "ALTER TABLE ~a ADD ~a;"
               (format-table-name table)
               (pgsql-index-condef index)))

      (t
       (or (pgsql-index-sql index)
           (format nil "CREATE~:[~; UNIQUE~] INDEX ~@[~a.~]~a ON ~a (~{~a~^, ~});"
                   (pgsql-index-unique index)
                   (pgsql-index-schema index)
                   index-name
                   (format-table-name table)
                   (pgsql-index-columns index)))))))

(defmethod format-pgsql-drop-index ((table table) (index pgsql-index))
  "Generate the PostgreSQL statement to DROP the index."
  (let* ((schema-name (apply-identifier-case (pgsql-index-schema index)))
         (index-name  (apply-identifier-case (pgsql-index-name index))))
    (cond ((pgsql-index-conname index)
           ;; here always quote the constraint name, currently the name
           ;; comes from one source only, the PostgreSQL database catalogs,
           ;; so don't question it, quote it.
           (format nil "ALTER TABLE ~a DROP CONSTRAINT ~s;"
                   (format-table-name table)
                   (pgsql-index-conname index)))

          (t
           (format nil "DROP INDEX ~@[~a.~]~a;" schema-name index-name)))))

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
       :collect (multiple-value-bind (sql pkey)
                    ;; we postpone the pkey upgrade of the index for later.
                    (format-pgsql-create-index table index)

                  (log-message :notice "~a" sql)
                  (lp:submit-task channel
                                  #'pgsql-connect-and-execute-with-timing
                                  ;; each thread must have its own connection
                                  (clone-connection pgconn)
                                  :post label sql)

                  ;; return the pkey "upgrade" statement
                  pkey))))

;;;
;;; Protect from non-unique index names
;;;
(defun set-table-oids (catalog)
  "MySQL allows using the same index name against separate tables, which
   PostgreSQL forbids. To get unicity in index names without running out of
   characters (we are allowed only 63), we use the table OID instead.

   This function grabs the table OIDs in the PostgreSQL database and update
   the definitions with them."
  (let* ((table-names (mapcar #'format-table-name (table-list catalog)))
	 (table-oids  (pgloader.pgsql:list-table-oids table-names)))
    (loop :for table :in (table-list catalog)
       :for table-name := (format-table-name table)
       :for table-oid := (cdr (assoc table-name table-oids :test #'string=))
       :unless table-oid :do (error "OID not found for ~s." table-name)
       :do (setf (table-oid table) table-oid)
           (loop :for index :in (table-index-list table)
              :do (setf (pgsql-index-table-oid index) table-oid)))))

;;;
;;; Drop indexes before loading
;;;
(defun drop-indexes (section table)
  "Drop indexes in PGSQL-INDEX-LIST. A PostgreSQL connection must already be
   active when calling that function."
  (loop :for index :in (table-index-list table)
     :do (let ((sql (format-pgsql-drop-index table index)))
           (log-message :notice "~a" sql)
           (pgsql-execute-with-timing section "drop indexes" sql))))

;;;
;;; Higher level API to care about indexes
;;;
(defun maybe-drop-indexes (target table &key (section :pre) drop-indexes)
  "Drop the indexes for TABLE-NAME on TARGET PostgreSQL connection, and
   returns a list of indexes to create again."
  (with-pgsql-connection (target)
    (let ((indexes (list-indexes table))
          ;; we get the list of indexes from PostgreSQL catalogs, so don't
          ;; question their spelling, just quote them.
          (*identifier-case* :quote))

      ;; set the indexes list in the table structure
      (setf (table-index-list table) indexes)

      (cond ((and indexes (not drop-indexes))
             (log-message :warning
                          "Target table ~s has ~d indexes defined against it."
                          (format-table-name table) (length indexes))
             (log-message :warning
                          "That could impact loading performance badly.")
             (log-message :warning
                          "Consider the option 'drop indexes'."))

            (indexes
             ;; drop the indexes now
             (with-stats-collection ("drop indexes" :section section)
                 (drop-indexes section table)))))))

(defun create-indexes-again (target table &key (section :post) drop-indexes)
  "Create the indexes that we dropped previously."
  (when (and (table-index-list table) drop-indexes)
    (let* ((*preserve-index-names* t)
           ;; we get the list of indexes from PostgreSQL catalogs, so don't
           ;; question their spelling, just quote them.
           (*identifier-case* :quote)
           (idx-kernel  (make-kernel (count-indexes table)))
           (idx-channel (let ((lp:*kernel* idx-kernel))
                          (lp:make-channel))))
      (let ((pkeys
             (create-indexes-in-kernel target table idx-kernel idx-channel)))

        (with-stats-collection ("Index Build Completion" :section section)
            (loop :repeat (count-indexes table)
               :do (lp:receive-result idx-channel))
          (lp:end-kernel :wait t))

        ;; turn unique indexes into pkeys now
        (with-pgsql-connection (target)
          (with-stats-collection ("Constraints" :section section)
              (loop :for sql :in pkeys
                 :when sql
                 :do (progn
                       (log-message :notice "~a" sql)
                       (pgsql-execute-with-timing section "Constraints" sql)))))))))

;;;
;;; Sequences
;;;
(defun reset-sequences (catalog &key pgconn (section :post))
  "Reset all sequences created during this MySQL migration."
  (log-message :notice "Reset sequences")
  (with-stats-collection ("Reset Sequences"
                          :use-result-as-rows t
                          :section section)
      (reset-all-sequences pgconn :tables (table-list catalog))))


;;;
;;; Comments
;;;
(defun comment-on-tables-and-columns (catalog)
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
                               :collect (+ (random 26) (char-code #\A)))))))
    (with-stats-collection ("Install comments"
                            :use-result-as-rows t
                            :section :post)
        (loop :for table :in (table-list catalog)
           :for sql := (when (table-comment table)
                         (format nil "comment on table ~a is $~a$~a$~a$"
                                 (table-name table)
                                 quote (table-comment table) quote))
           :count (when sql
                    (log-message :notice "~a" sql)
                    (pgsql-execute-with-timing :post "Comments" sql))

           :sum (loop :for column :in (table-column-list table)
                   :for sql := (when (column-comment column)
                                 (format nil "comment on column ~a.~a is $~a$~a$~a$"
                                         (table-name table)
                                         (column-name column)
                                         quote (column-comment column) quote))
                   :count (when sql
                            (log-message :notice "~a;" sql)
                            (pgsql-execute-with-timing :post "Comments" sql)))))))

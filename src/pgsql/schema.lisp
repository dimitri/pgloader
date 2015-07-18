;;;
;;; Tools to handle PostgreSQL tables and indexes creations
;;;
(in-package pgloader.pgsql)

(defvar *pgsql-reserved-keywords* nil
  "We need to always quote PostgreSQL reserved keywords")

(defun quoted-p (s)
  "Return true if s is a double-quoted string"
  (and (eq (char s 0) #\")
       (eq (char s (- (length s) 1)) #\")))

(defun apply-identifier-case (identifier)
  "Return given IDENTIFIER with CASE handled to be PostgreSQL compatible."
  (let* ((lowercase-identifier (cl-ppcre:regex-replace-all
                                "[^a-zA-Z0-9.]" (string-downcase identifier) "_"))
         (*identifier-case*
          ;; we might need to force to :quote in some cases
          ;;
          ;; http://www.postgresql.org/docs/9.1/static/sql-syntax-lexical.html
          ;;
          ;; SQL identifiers and key words must begin with a letter (a-z, but
          ;; also letters with diacritical marks and non-Latin letters) or an
          ;; underscore (_).
          (cond ((quoted-p identifier)
                 :none)

                ((cl-ppcre:scan "^[^A-Za-z_]" identifier)
                 :quote)

                ((member lowercase-identifier *pgsql-reserved-keywords*
                         :test #'string=)
                 (progn
                   ;; we need to both downcase and quote here
                   (when (eq :downcase *identifier-case*)
                     (setf identifier lowercase-identifier))
                   :quote))

                ;; in other cases follow user directive
                (t *identifier-case*))))

    (ecase *identifier-case*
      (:downcase lowercase-identifier)
      (:quote    (format nil "\"~a\""
                         (cl-ppcre:regex-replace-all "\"" identifier "\"\"")))
      (:none     identifier))))

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

(defmethod format-pgsql-column ((col pgsql-column))
  "Return a string representing the PostgreSQL column definition."
  (let* ((column-name
	  (apply-identifier-case (pgsql-column-name col)))
	 (type-definition
	  (format nil
		  "~a~@[~a~]~:[~; not null~]~@[ default ~a~]"
		  (pgsql-column-type-name col)
		  (pgsql-column-type-mod col)
		  (pgsql-column-nullable col)
		  (pgsql-column-default col))))
    (format nil "~a ~22t ~a" column-name type-definition)))

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
  (let* ((constraint-name (apply-identifier-case (pgsql-fkey-name fk)))
	 (table-name      (apply-identifier-case (pgsql-fkey-table-name fk)))
         (fkey-columns    (mapcar (lambda (column-name)
                                    (apply-identifier-case column-name))
                                  (pgsql-fkey-columns fk)))
	 (foreign-table   (apply-identifier-case (pgsql-fkey-foreign-table fk)))
         (foreign-columns (mapcar #'apply-identifier-case
                                  (pgsql-fkey-foreign-columns fk))))
    (format nil
	    "ALTER TABLE ~a ADD CONSTRAINT ~a FOREIGN KEY(~{~a~^,~}) REFERENCES ~a(~{~a~^,~})~:[~*~; ON UPDATE ~a~]~:[~*~; ON DELETE ~a~]"
	    table-name
	    constraint-name
	    fkey-columns
	    foreign-table
	    foreign-columns
            (pgsql-fkey-update-rule fk)
            (pgsql-fkey-update-rule fk)
            (pgsql-fkey-delete-rule fk)
            (pgsql-fkey-delete-rule fk))))

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

(defun drop-pgsql-fkeys (all-fkeys)
  "Drop all Foreign Key Definitions given, to prepare for a clean run."
  (let ((all-pgsql-fkeys (list-tables-and-fkeys)))
    (loop for (table-name . fkeys) in all-fkeys
       do
	 (loop for fkey in fkeys
	    for sql = (format-pgsql-drop-fkey fkey
					      :all-pgsql-fkeys all-pgsql-fkeys)
	    when sql
	    do
	      (log-message :notice "~a;" sql)
	      (pgsql-execute sql)))))

(defun create-pgsql-fkeys (pgconn all-fkeys
                           &key
                             state
                             (label "Foreign Keys"))
  "Actually create the Foreign Key References that where declared in the
   MySQL database"
  (pgstate-add-table state (db-name pgconn) label)
  (loop for (table-name . fkeys) in all-fkeys
     do (loop for fkey in fkeys
	   for sql = (format-pgsql-create-fkey fkey)
	   do
	     (log-message :notice "~a;" sql)
	     (pgsql-execute-with-timing "Foreign Keys" sql state))))


;;;
;;; Table schema rewriting support
;;;
(defun create-table-sql (table-name cols &key if-not-exists)
  "Return a PostgreSQL CREATE TABLE statement from given COLS.

   Each element of the COLS list is expected to be of a type handled by the
   `format-pgsql-column' generic function."
  (with-output-to-string (s)
    (let ((table-name (apply-identifier-case table-name)))
      (format s "CREATE TABLE~:[~; IF NOT EXISTS~] ~a ~%(~%"
	      if-not-exists
	      table-name))
    (loop
       for (col . last?) on cols
       for pg-coldef = (format-pgsql-column col)
       do (format s "  ~a~:[~;,~]~%" pg-coldef last?))
    (format s ");~%")))

(defun drop-table-if-exists-sql (table-name)
  "Return the PostgreSQL DROP TABLE IF EXISTS statement for TABLE-NAME."
  (let ((table-name (apply-identifier-case table-name)))
    (format nil "DROP TABLE IF EXISTS ~a~% CASCADE;" table-name)))

(defun create-table-sql-list (all-columns
			      &key
				if-not-exists
				include-drop)
  "Return the list of CREATE TABLE statements to run against PostgreSQL.

   The ALL-COLUMNS parameter must be a list of alist associations where the
   car is the table-name (a string) and the cdr is a column list. Each
   element of the column list is expected to be of a type handled by the
   `format-pgsql-column' generic function, such as `pgsql-column'."
  (loop
     for (table-name . cols) in all-columns
     for extra-types = (loop for col in cols
			  append (format-extra-type col
						    :include-drop include-drop))

     when include-drop
     collect (drop-table-if-exists-sql table-name)

     when extra-types append extra-types

     collect (create-table-sql table-name cols :if-not-exists if-not-exists)))

(defun create-tables (all-columns
		      &key
			if-not-exists
			include-drop
			(client-min-messages :notice))
  "Create all tables in database dbname in PostgreSQL.

   The ALL-COLUMNS parameter must be a list of alist associations where the
   car is the table-name (a string) and the cdr is a column list. Each
   element of the column list is expected to be of a type handled by the
   `format-pgsql-column' generic function, such as `pgsql-column'."
  (loop
     for sql in (create-table-sql-list all-columns
				       :if-not-exists if-not-exists
				       :include-drop include-drop)
     count (not (null sql)) into nb-tables
     when sql
     do
       (log-message :info "~a" sql)
       (pgsql-execute sql :client-min-messages client-min-messages)
     finally (return nb-tables)))

(defun truncate-tables (pgconn table-name-list)
  "Truncate given TABLE-NAME in database DBNAME"
  (with-pgsql-transaction (:pgconn pgconn)
    (flet ((process-table-name (table-name)
             (typecase table-name
               (cons
                (format nil "~a.~a"
                        (apply-identifier-case (car table-name))
                        (apply-identifier-case (cdr table-name))))
               (string
                (apply-identifier-case table-name)))))
      (let ((sql (format nil "TRUNCATE ~{~a~^,~};"
                         (mapcar #'process-table-name table-name-list))))
        (log-message :notice "~a" sql)
        (pomo:execute sql)))))

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


;;;
;;; Index support
;;;
(defstruct pgsql-index
  ;; the struct is used both for supporting new index creation from non
  ;; PostgreSQL system and for drop/create indexes when using the 'drop
  ;; indexes' option (in CSV mode and the like)
  name table-name table-oid primary unique columns sql conname condef)

(defgeneric index-table-name (index)
  (:documentation
   "Return the name of the table to attach this index to."))

(defgeneric format-pgsql-create-index (index)
  (:documentation
   "Return the PostgreSQL command to define an Index."))

(defmethod index-table-name ((index pgsql-index))
  (pgsql-index-table-name index))

(defmethod format-pgsql-create-index ((index pgsql-index))
  "Generate the PostgreSQL statement list to rebuild a Foreign Key"
  (let* ((index-name (if (and *preserve-index-names*
                              (not (string-equal "primary" (pgsql-index-name index)))
                              (pgsql-index-table-oid index))
                         (pgsql-index-name index)

                         ;; in the general case, we build our own index name.
                         (format nil "idx_~a_~a"
                                 (pgsql-index-table-oid index)
                                 (pgsql-index-name index))))
	 (table-name (apply-identifier-case (pgsql-index-table-name index)))
	 (index-name (apply-identifier-case index-name))

	 (cols (mapcar #'apply-identifier-case (pgsql-index-columns index))))
    (cond
      ((or (pgsql-index-primary index)
           (and (pgsql-index-condef index) (pgsql-index-unique index)))
       (values
        ;; ensure good concurrency here, don't take the ACCESS EXCLUSIVE
        ;; LOCK on the table before we have the index done already
        (or (pgsql-index-sql index)
            (format nil "CREATE UNIQUE INDEX ~a ON ~a (~{~a~^, ~});"
                    index-name table-name cols))
        (format nil
                "ALTER TABLE ~a ADD ~a USING INDEX ~a;"
                table-name
                (cond ((pgsql-index-primary index) "PRIMARY KEY")
                      ((pgsql-index-unique index) "UNIQUE"))
                index-name)))

      ((pgsql-index-condef index)
       (format nil "ALTER TABLE ~a ADD ~a;"
               table-name (pgsql-index-condef index)))

      (t
       (or (pgsql-index-sql index)
           (format nil "CREATE~:[~; UNIQUE~] INDEX ~a ON ~a (~{~a~^, ~});"
                   (pgsql-index-unique index)
                   index-name
                   table-name
                   cols))))))

(defmethod format-pgsql-drop-index ((index pgsql-index))
  "Generate the PostgreSQL statement to DROP the index."
  (let* ((table-name (apply-identifier-case (pgsql-index-table-name index)))
	 (index-name (apply-identifier-case (pgsql-index-name index))))
    (cond ((pgsql-index-conname index)
           (format nil "ALTER TABLE ~a DROP CONSTRAINT ~a;"
                   table-name (pgsql-index-conname index)))

          (t
           (format nil "DROP INDEX ~a;" index-name)))))

;;;
;;; Parallel index building.
;;;
(defun create-indexes-in-kernel (pgconn indexes kernel channel
				 &key
                                   state
				   (label "Create Indexes"))
  "Create indexes for given table in dbname, using given lparallel KERNEL
   and CHANNEL so that the index build happen in concurrently with the data
   copying."
  (let* ((lp:*kernel* kernel))
    ;; ensure we have a stats entry
    (pgstate-add-table state (db-name pgconn) label)

    (loop
       :for index :in indexes
       :collect (multiple-value-bind (sql pkey)
                    ;; we postpone the pkey upgrade of the index for later.
                    (format-pgsql-create-index index)

                  (log-message :notice "~a" sql)
                  (lp:submit-task channel
                                  #'pgsql-connect-and-execute-with-timing
                                  ;; each thread must have its own connection
                                  (new-pgsql-connection pgconn)
                                  label sql state)

                  ;; return the pkey "upgrade" statement
                  pkey))))

;;;
;;; Protect from non-unique index names
;;;
(defun set-table-oids (all-indexes)
  "MySQL allows using the same index name against separate tables, which
   PostgreSQL forbids. To get unicity in index names without running out of
   characters (we are allowed only 63), we use the table OID instead.

   This function grabs the table OIDs in the PostgreSQL database and update
   the definitions with them."
  (let* ((table-names (mapcar #'apply-identifier-case
                              (mapcar #'car all-indexes)))
	 (table-oids  (pgloader.pgsql:list-table-oids table-names)))
    (loop for (table-name-raw . indexes) in all-indexes
       for table-name = (apply-identifier-case table-name-raw)
       for table-oid = (cdr (assoc table-name table-oids :test #'string=))
       unless table-oid do (error "OID not found for ~s." table-name)
       do (loop for index in indexes
	     do (setf (pgsql-index-table-oid index) table-oid)))))

;;;
;;; Drop indexes before loading
;;;
(defun drop-indexes (state pgsql-index-list)
  "Drop indexes in PGSQL-INDEX-LIST. A PostgreSQL connection must already be
   active when calling that function."
  (loop :for index :in pgsql-index-list
     :do (let ((sql (format-pgsql-drop-index index)))
           (log-message :notice "~a" sql)
           (pgsql-execute-with-timing "drop indexes" sql state))))

;;;
;;; Higher level API to care about indexes
;;;
(defun maybe-drop-indexes (target table-name state &key drop-indexes)
  "Drop the indexes for TABLE-NAME on TARGET PostgreSQL connection, and
   returns a list of indexes to create again."
  (with-pgsql-connection (target)
    (let ((indexes (list-indexes table-name)))
      (cond ((and indexes (not drop-indexes))
             (log-message :warning
                          "Target table ~s has ~d indexes defined against it."
                          table-name (length indexes))
             (log-message :warning
                          "That could impact loading performance badly.")
             (log-message :warning
                          "Consider the option 'drop indexes'."))

            (indexes
             ;; drop the indexes now
             (with-stats-collection ("drop indexes" :state state)
                 (drop-indexes state indexes))))

      ;; and return the indexes list
      indexes)))

(defun create-indexes-again (target indexes state state-parallel
                             &key drop-indexes)
  "Create the indexes that we dropped previously."
  (when (and indexes drop-indexes)
    (let* ((*preserve-index-names* t)
           (idx-kernel  (make-kernel (length indexes)))
           (idx-channel (let ((lp:*kernel* idx-kernel))
                          (lp:make-channel))))
      (let ((pkeys
             (create-indexes-in-kernel target indexes idx-kernel idx-channel
                                       :state state-parallel)))

        (with-stats-collection ("Index Build Completion" :state state)
            (loop :for idx :in indexes :do (lp:receive-result idx-channel)))

        ;; turn unique indexes into pkeys now
        (with-pgsql-connection (target)
          (with-stats-collection ("Constraints" :state state)
              (loop :for sql :in pkeys
                 :when sql
                 :do (progn
                       (log-message :notice "~a" sql)
                       (pgsql-execute-with-timing "Constraints" sql state)))))))))

;;;
;;; Sequences
;;;
(defun reset-sequences (table-names &key pgconn state)
  "Reset all sequences created during this MySQL migration."
  (log-message :notice "Reset sequences")
  (with-stats-collection ("Reset Sequences"
                          :dbname (db-name pgconn)
                          :use-result-as-rows t
                          :state state)
    (reset-all-sequences pgconn :tables table-names)))

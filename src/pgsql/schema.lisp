;;;
;;; Tools to handle PostgreSQL tables and indexes creations
;;;
(in-package pgloader.pgsql)

(defvar *pgsql-reserved-keywords* nil
  "We need to always quote PostgreSQL reserved keywords")

(defun apply-identifier-case (identifier)
  "Return given IDENTIFIER with CASE handled to be PostgreSQL compatible."
  (ecase *identifier-case*
    (:downcase (let ((lowered (cl-ppcre:regex-replace-all
                                "[^a-zA-Z0-9.]" (string-downcase identifier) "_")))
                 (if (member lowered *pgsql-reserved-keywords* :test #'string=)
                   (format nil "\"~a\"" lowered)
                   lowered)))
    (:quote    (format nil "\"~a\"" identifier))
    (:none     identifier)))

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
  name table-name columns foreign-table foreign-columns)

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
	    "ALTER TABLE ~a ADD CONSTRAINT ~a FOREIGN KEY(~{~a~^,~}) REFERENCES ~a(~{~a~^,~})"
	    table-name
	    constraint-name
	    fkey-columns
	    foreign-table
	    foreign-columns)))

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

(defun drop-pgsql-fkeys (all-fkeys &key (dbname *pg-dbname*))
  "Drop all Foreign Key Definitions given, to prepare for a clean run."
  (let ((all-pgsql-fkeys (list-tables-and-fkeys dbname)))
    (loop for (table-name . fkeys) in all-fkeys
       do
	 (loop for fkey in fkeys
	    for sql = (format-pgsql-drop-fkey fkey
					      :all-pgsql-fkeys all-pgsql-fkeys)
	    when sql
	    do
	      (log-message :notice "~a;" sql)
	      (pgsql-execute sql)))))

(defun create-pgsql-fkeys (all-fkeys
                           &key
                             (dbname *pg-dbname*)
                             state
                             (label "Foreign Keys"))
  "Actually create the Foreign Key References that where declared in the
   MySQL database"
  (pgstate-add-table state dbname label)
  (loop for (table-name . fkeys) in all-fkeys
     do (loop for fkey in fkeys
	   for sql = (format-pgsql-create-fkey fkey)
	   do
	     (log-message :notice "~a;" sql)
	     (pgsql-execute-with-timing dbname "Foreign Keys" sql state))))


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
    (format nil "DROP TABLE IF EXISTS ~a;~%" table-name)))

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

(defun truncate-tables (dbname table-name-list)
  "Truncate given TABLE-NAME in database DBNAME"
  (with-pgsql-transaction (:dbname dbname)
    (let ((sql (format nil "TRUNCATE ~{~a~^,~};"
                       (loop :for table-name :in table-name-list
                          :collect (apply-identifier-case table-name)))))
      (log-message :notice "~a" sql)
      (pomo:execute sql))))


;;;
;;; Index support
;;;
(defstruct pgsql-index name table-name table-oid primary unique columns)

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
  (let* ((index-name (format nil "idx_~a_~a"
			     (pgsql-index-table-oid index)
			     (pgsql-index-name index)))
	 (table-name (apply-identifier-case (pgsql-index-table-name index)))
	 (index-name (apply-identifier-case index-name))

	 (cols (mapcar #'apply-identifier-case (pgsql-index-columns index))))
    (cond
      ((pgsql-index-primary index)
       (values
        ;; ensure good concurrency here, don't take the ACCESS EXCLUSIVE
        ;; LOCK on the table before we have the index done already
        (format nil "CREATE UNIQUE INDEX ~a ON ~a (~{~a~^, ~});"
                index-name table-name cols)
        (format nil
                "ALTER TABLE ~a ADD PRIMARY KEY USING INDEX ~a;"
                table-name index-name)))

      (t
       (format nil "CREATE~:[~; UNIQUE~] INDEX ~a ON ~a (~{~a~^, ~});"
               (pgsql-index-unique index)
               index-name
               table-name
               cols)))))

;;;
;;; Parallel index building.
;;;
(defun create-indexes-in-kernel (dbname indexes kernel channel
				 &key
                                   state
				   (label "Create Indexes"))
  "Create indexes for given table in dbname, using given lparallel KERNEL
   and CHANNEL so that the index build happen in concurrently with the data
   copying."
  (let* ((lp:*kernel* kernel))
    ;; ensure we have a stats entry
    (pgstate-add-table state dbname label)

    (loop
       :for index :in indexes
       :collect (multiple-value-bind (sql pkey)
                    ;; we postpone the pkey upgrade of the index for later.
                    (format-pgsql-create-index index)

                  (log-message :notice "~a" sql)
                  (lp:submit-task channel #'pgsql-execute-with-timing
                                  dbname label sql state)

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
  (log-message :debug "set-table-oids: ~s" all-indexes)
  (let* ((table-names (mapcar #'apply-identifier-case
                              (mapcar #'car all-indexes)))
	 (table-oids  (pgloader.pgsql:list-table-oids table-names)))
    (loop for (table-name-raw . indexes) in all-indexes
       for table-name = (apply-identifier-case table-name-raw)
       for table-oid = (cdr (assoc table-name table-oids :test #'string=))
       unless table-oid do (error "OID not found for ~s." table-name)
       do (loop for index in indexes
	     do (setf (pgloader.pgsql::pgsql-index-table-oid index) table-oid)))))



;;;
;;; Sequences
;;;
(defun reset-sequences (table-names
                              &key (dbname *pg-dbname*) state)
  "Reset all sequences created during this MySQL migration."
  (log-message :notice "Reset sequences")
  (with-stats-collection ("Reset Sequences"
                          :dbname dbname
                          :use-result-as-rows t
                          :state state)
    (reset-all-sequences dbname :tables table-names)))

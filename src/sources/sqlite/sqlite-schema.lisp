;;;
;;; SQLite tools connecting to a database
;;;
(in-package :pgloader.source.sqlite)

(defvar *sqlite-db* nil
  "The SQLite database connection handler.")

;;;
;;; Integration with the pgloader Source API
;;;
(defclass sqlite-connection (fd-connection)
  ((has-sequences :initform nil :accessor has-sequences)))

(defmethod initialize-instance :after ((slconn sqlite-connection) &key)
  "Assign the type slot to sqlite."
  (setf (slot-value slconn 'type) "sqlite"))

(defmethod open-connection ((slconn sqlite-connection) &key check-has-sequences)
  (setf (conn-handle slconn)
        (sqlite:connect (fd-path slconn)))
  (log-message :debug "CONNECTED TO ~a" (fd-path slconn))
  (when check-has-sequences
    (let ((sql (format nil (sql "/sqlite/sqlite-sequence.sql"))))
      (log-message :sql "SQLite: ~a" sql)
      (when (sqlite:execute-single (conn-handle slconn) sql)
        (setf (has-sequences slconn) t))))
  slconn)

(defmethod close-connection ((slconn sqlite-connection))
  (sqlite:disconnect (conn-handle slconn))
  (setf (conn-handle slconn) nil)
  slconn)

(defmethod clone-connection ((slconn sqlite-connection))
  (change-class (call-next-method slconn) 'sqlite-connection))

(defmethod query ((slconn sqlite-connection) sql &key)
  (log-message :sql "SQLite: sending query: ~a" sql)
  (sqlite:execute-to-list (conn-handle slconn) sql))


;;;
;;; SQLite schema introspection facilities
;;;
(defun filter-list-to-where-clause (filter-list
                                    &optional
                                      not
                                      (table-col  "tbl_name"))
  "Given an INCLUDING or EXCLUDING clause, turn it into a SQLite WHERE clause."
  (mapcar (lambda (table-name)
            (format nil "(~a ~:[~;NOT ~]LIKE '~a')"
                    table-col not table-name))
          filter-list))

(defun list-tables (&key
                      (db *sqlite-db*)
                      including
                      excluding)
  "Return the list of tables found in SQLITE-DB."
  (let ((sql (format nil (sql "/sqlite/list-tables.sql")
                     including          ; do we print the clause?
                     (filter-list-to-where-clause including nil)
                     excluding          ; do we print the clause?
                     (filter-list-to-where-clause excluding t))))
    (log-message :sql "~a" sql)
    (loop for (name) in (sqlite:execute-to-list db sql)
       collect name)))

(defun find-sequence (db table-name column-name)
  "Find if table-name.column-name is attached to a sequence in
   sqlite_sequence catalog."
  (let* ((sql (format nil (sql "/sqlite/find-sequence.sql") table-name))
         (seq (sqlite:execute-single db sql)))
    (when (and seq (not (zerop seq)))
      ;; magic marker for `apply-casting-rules'
      (log-message :notice "SQLite column ~a.~a uses a sequence"
                   table-name column-name)
      seq)))

(defun find-auto-increment-in-create-sql (db table-name column-name)
  "The sqlite_sequence catalog is only created when some content has been
   added to the table. So we might fail to FIND-SEQUENCE, and still need to
   consider the column has an autoincrement. Parse the SQL definition of the
   table to find out."
  (let* ((sql (format nil (sql "/sqlite/get-create-table.sql") table-name))
         (create-table (sqlite:execute-single db sql))
         (open-paren   (+ 1 (position #\( create-table)))
         (close-paren  (position #\) create-table :from-end t))
         (coldefs
          (mapcar (lambda (def) (string-trim (list #\Space) def))
                  (split-sequence:split-sequence #\,
                                                 create-table
                                                 :start open-paren
                                                 :end close-paren))))
    (loop :for coldef :in coldefs
       :do (let* ((words (mapcar (lambda (w) (string-trim '(#\" #\') w))
                                 (split-sequence:split-sequence #\Space coldef)))
                  (colname (first words))
                  (props   (rest words)))
             (when (and (string= colname column-name)
                        (member "autoincrement" props :test #'string-equal))
               ;; we know the target column has no sequence because we
               ;; looked into that first by calling find-sequence, and we
               ;; only call find-auto-increment-in-create-sql when
               ;; find-sequence failed to find anything.
               (log-message :notice "SQLite column ~a.~a is autoincrement, but has no sequence"
                            table-name column-name)
               (return t))))))

(defun list-columns (table &key db-has-sequences (db *sqlite-db*) )
  "Return the list of columns found in TABLE-NAME."
  (let* ((table-name (table-source-name table))
         (sql        (format nil (sql "/sqlite/list-columns.sql") table-name)))
    (loop :for (ctid name type nullable default pk-id)
       :in (sqlite:execute-to-list db sql)
       :do (let* ((ctype (normalize type))
                  (dtype (ctype-to-dtype type))
                  (field (make-coldef table-name
                                      ctid
                                      name
                                      dtype
                                      ctype
                                      (= 1 nullable)
                                      (unquote default)
                                      pk-id)))
             (when (and db-has-sequences
                        (not (zerop pk-id))
                        (string-equal (coldef-ctype field) "integer")
                        (or (find-sequence db table-name name)
                            (find-auto-increment-in-create-sql db
                                                               table-name
                                                               name)))
               ;; then it might be an auto_increment, which we know by
               ;; looking at the sqlite_sequence catalog
               (setf (coldef-extra field) :auto-increment))
             (add-field table field)))))

(defun list-all-columns (schema
                         &key
                           db-has-sequences
                           (db *sqlite-db*)
                           including
                           excluding)
  "Get the list of SQLite column definitions per table."
  (loop :for table-name :in (list-tables :db db
                                         :including including
                                         :excluding excluding)
     :do (let ((table (add-table schema table-name)))
           (list-columns table :db db :db-has-sequences db-has-sequences))))


;;;
;;; Index support
;;;
(defun add-unlisted-primary-key-index (table)
  "Add to TABLE any unlisted primary key index..."
  (when (notany #'index-primary (table-index-list table))
    (let ((pk-fields (loop :for field :in (table-field-list table)
                        :when (< 0 (coldef-pk-id field))
                        :collect field)))
      (when (and pk-fields
                 ;; we don't know if that holds true for non-integer fields,
                 ;; as it appears to be tied to the rowid magic column
                 (every (lambda (field)
                          (string-equal "integer" (coldef-dtype field)))
                        pk-fields))
        (let ((pk-name (build-identifier "_" (format-table-name table) "pkey"))
              (clist   (mapcar #'coldef-name pk-fields)))
          ;; now forge the index and get it a name
          (add-index table (make-index :name pk-name
                                       :table table
                                       :primary t
                                       :unique t
                                       :columns clist)))))))

(defun is-index-pk (table index-col-name-list)
  "The only way to know with SQLite pragma introspection if a particular
   UNIQUE index is actually PRIMARY KEY is by comparing the list of column
   names in the index with the ones marked with non-zero pk in the table
   definition."
  (equal (loop :for field :in (table-field-list table)
            :when (< 0 (coldef-pk-id field))
            :collect (coldef-name field))
         index-col-name-list))

(defun list-index-cols (index-name &optional (db *sqlite-db*))
  "Return the list of columns in INDEX-NAME."
  (let ((sql (format nil (sql "/sqlite/list-index-cols.sql") index-name)))
    (loop :for (index-pos table-pos col-name) :in (sqlite:execute-to-list db sql)
       :collect (apply-identifier-case col-name))))

(defun list-indexes (table &optional (db *sqlite-db*))
  "Return the list of indexes attached to TABLE."
  (let* ((table-name (table-source-name table))
         (sql
          (format nil (sql "/sqlite/list-table-indexes.sql") table-name)))
    (loop
       :for (seq index-name unique origin partial)
       :in (sqlite:execute-to-list db sql)
       :do (let* ((cols  (list-index-cols index-name db))
                  (index (make-index :name index-name
                                     :table table
                                     :primary (is-index-pk table cols)
                                     :unique (= unique 1)
                                     :columns cols)))
             (add-index table index))))

  ;; ok that's not the whole story. Integer columns marked pk=1 are actually
  ;; primary keys but the supporting index isn't listed in index_list()
  ;;
  ;; we add unlisted pkeys only after having read the catalogs, otherwise we
  ;; might create double primary key indexes here
  (add-unlisted-primary-key-index table))

(defun list-all-indexes (schema &key (db *sqlite-db*))
  "Get the list of SQLite index definitions per table."
  (loop :for table :in (schema-table-list schema)
     :do (list-indexes table db)))


;;;
;;; Foreign keys support
;;;
(defun list-fkeys (table &optional (db *sqlite-db*))
  "Return the list of indexes attached to TABLE."
  (let* ((table-name (table-source-name table))
         (sql
          (format nil (sql "/sqlite/list-fkeys.sql") table-name)))
    (loop
       :with fkey-table := (make-hash-table)
       :for (id seq ftable-name from to on-update on-delete match)
       :in (sqlite:execute-to-list db sql)

       :do (let* ((ftable (find-table (table-schema table) ftable-name))
                  (fkey   (or (gethash id fkey-table)
                              (when ftable
                                (let ((pg-fkey
                                       (make-fkey :table table
                                                  :columns nil
                                                  :foreign-table ftable
                                                  :foreign-columns nil
                                                  :update-rule on-update
                                                  :delete-rule on-delete)))
                                  (setf (gethash id fkey-table) pg-fkey)
                                  (add-fkey table pg-fkey)
                                  pg-fkey)))))
             (if (and fkey from to)
                 (progn
                   (push-to-end from (fkey-columns fkey))
                   (push-to-end to   (fkey-foreign-columns fkey)))

                 ;; it might be INCLUDING/EXCLUDING clauses that make it we
                 ;; don't have to care about the fkey definition, or it
                 ;; might be that the SQLite fkey definition is missing
                 ;; information, such as the `to` column, as seen in the
                 ;; field (bug report #681)
                 (log-message :info
                              "Incomplete Foreign Key definition on table ~a(~a) referencing table ~a(~a)"
                              (when table (format-table-name table))
                              from
                              (when ftable (format-table-name ftable))
                              to))))))

(defun list-all-fkeys (schema &key (db *sqlite-db*))
  "Get the list of SQLite foreign keys definitions per table."
  (loop :for table :in (schema-table-list schema)
     :do (list-fkeys table db)))

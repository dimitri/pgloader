;;;
;;; SQLite tools connecting to a database
;;;
(in-package :pgloader.source.sqlite)

(defclass copy-sqlite (db-copy)
  ((db :accessor db :initarg :db))
  (:documentation "pgloader SQLite Data Source"))

;;;
;;; SQLite schema introspection facilities
;;;
(defun sqlite-pragma-encoding (db)
  (handler-case
      (sqlite:execute-single db "pragma encoding;")
    (sqlite:sqlite-error (e)
      (if (eq :busy (sqlite:sqlite-error-code e))
          ;; retry when "database is locked" for being BUSY
          (progn
            (sleep 0.1)
            (sqlite-pragma-encoding db))
          ;; fail by re-signaling the error
          (error e)))))

(defun sqlite-encoding (db)
  "Return a BABEL suitable encoding for the SQLite db handle."
  (let ((encoding-string (sqlite-pragma-encoding db)))
    (cond ((string-equal encoding-string "UTF-8")    :utf-8)
          ((string-equal encoding-string "UTF-16")   :utf-16)
          ((string-equal encoding-string "UTF-16le") :utf-16le)
          ((string-equal encoding-string "UTF-16be") :utf-16be))))

(defmethod filter-list-to-where-clause ((sqlite copy-sqlite)
                                        filter-list
                                        &key
                                          not
                                          (table-col  "tbl_name")
                                          &allow-other-keys)
  "Given an INCLUDING or EXCLUDING clause, turn it into a SQLite WHERE clause."
  (mapcar (lambda (table-name)
            (format nil "(~a ~:[~;NOT ~]LIKE '~a')"
                    table-col not table-name))
          filter-list))

(defun list-tables (sqlite
                    &key
                      (db *sqlite-db*)
                      including
                      excluding)
  "Return the list of tables found in SQLITE-DB."
  (let ((sql (sql "/sqlite/list-tables.sql"
                  including             ; do we print the clause?
                  (filter-list-to-where-clause sqlite including :not nil)
                  excluding             ; do we print the clause?
                  (filter-list-to-where-clause sqlite excluding :not t))))
    (log-message :sql "~a" sql)
    (loop for (name) in (sqlite:execute-to-list db sql)
       collect name)))

(defun find-sequence (db table-name column-name)
  "Find if table-name.column-name is attached to a sequence in
   sqlite_sequence catalog."
  (let* ((sql (sql "/sqlite/find-sequence.sql" table-name))
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
  (let* ((sql (sql "/sqlite/get-create-table.sql" table-name))
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
         (sql        (sql "/sqlite/list-columns.sql" table-name)))
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

(defmethod fetch-columns ((schema schema)
                          (sqlite copy-sqlite)
                          &key
                            db-has-sequences
                            table-type
                            including
                            excluding
                          &aux (db (conn-handle (source-db sqlite))))
  "Get the list of SQLite column definitions per table."
  (declare (ignore table-type))
  (loop :for table-name :in (list-tables sqlite
                                         :db db
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
  (let ((sql (sql "/sqlite/list-index-cols.sql" index-name)))
    (loop :for (index-pos table-pos col-name) :in (sqlite:execute-to-list db sql)
       :collect (apply-identifier-case col-name))))

(defun list-indexes (table &optional (db *sqlite-db*))
  "Return the list of indexes attached to TABLE."
  (let* ((table-name (table-source-name table))
         (sql (sql "/sqlite/list-table-indexes.sql" table-name)))
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

(defmethod fetch-indexes ((schema schema) (sqlite copy-sqlite)
                          &key &allow-other-keys
                          &aux (db (conn-handle (source-db sqlite))))
  "Get the list of SQLite index definitions per table."
  (loop :for table :in (schema-table-list schema)
     :do (list-indexes table db)))


;;;
;;; Foreign keys support
;;;
(defun list-fkeys (table &optional (db *sqlite-db*))
  "Return the list of indexes attached to TABLE."
  (let* ((table-name (table-source-name table))
         (sql (sql "/sqlite/list-fkeys.sql" table-name)))
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

(defmethod fetch-foreign-keys ((schema schema) (sqlite copy-sqlite)
                               &key &allow-other-keys
                               &aux (db (conn-handle (source-db sqlite))))
  "Get the list of SQLite foreign keys definitions per table."
  (loop :for table :in (schema-table-list schema)
     :do (list-fkeys table db)))

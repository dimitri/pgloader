;;;
;;; SQLite tools connecting to a database
;;;
(in-package :pgloader.sqlite)

(defvar *sqlite-db* nil
  "The SQLite database connection handler.")

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

(defmethod clone-connection ((slconn sqlite-connection))
  (change-class (call-next-method slconn) 'sqlite-connection))

(defmethod query ((slconn sqlite-connection) sql &key)
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
  (let ((sql (format nil "SELECT tbl_name
                FROM sqlite_master
               WHERE type='table'
                     AND tbl_name <> 'sqlite_sequence'
                     ~:[~*~;AND (~{~a~^~&~10t or ~})~]
                     ~:[~*~;AND (~{~a~^~&~10t and ~})~]"
                     including          ; do we print the clause?
                     (filter-list-to-where-clause including nil)
                     excluding          ; do we print the clause?
                     (filter-list-to-where-clause excluding t))))
    (log-message :info "~a" sql)
    (loop for (name) in (sqlite:execute-to-list db sql)
       collect name)))

(defun list-columns (table &optional (db *sqlite-db*))
  "Return the list of columns found in TABLE-NAME."
  (let* ((table-name (table-source-name table))
         (sql        (format nil "PRAGMA table_info(~a)" table-name)))
    (loop :for (seq name type nullable default pk-id) :in
       (sqlite:execute-to-list db sql)
       :do (let ((field (make-coldef table-name
                                     seq
                                     name
                                     (ctype-to-dtype (normalize type))
                                     (normalize type)
                                     (= 1 nullable)
                                     (unquote default)
                                     pk-id)))
             (add-field table field)))))

(defun list-all-columns (schema
                         &key
                           (db *sqlite-db*)
                           including
                           excluding)
  "Get the list of SQLite column definitions per table."
  (loop :for table-name :in (list-tables :db db
                                         :including including
                                         :excluding excluding)
     :do (let ((table (add-table schema table-name)))
           (list-columns table db))))


;;;
;;; Index support
;;;
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
  (let ((sql (format nil "PRAGMA index_info(~a)" index-name)))
    (loop :for (index-pos table-pos col-name) :in (sqlite:execute-to-list db sql)
       :collect col-name)))

(defun list-indexes (table &optional (db *sqlite-db*))
  "Return the list of indexes attached to TABLE."
  (let* ((table-name (table-source-name table))
         (sql        (format nil "PRAGMA index_list(~a)" table-name)))
    (loop
       :for (seq index-name unique origin partial) :in (sqlite:execute-to-list db sql)
       :do (let* ((cols  (list-index-cols index-name db))
                  (index (make-pgsql-index :name index-name
                                           :primary (is-index-pk table cols)
                                           :unique (= unique 1)
                                           :columns cols)))
             (add-index table index)))))

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
         (sql        (format nil "PRAGMA foreign_key_list(~a)" table-name)))
    (loop
       :with fkey-table := (make-hash-table)
       :for (id seq ftable-name from to on-update on-delete match)
       :in (sqlite:execute-to-list db sql)

       :do (let* ((ftable (find-table (table-schema table) ftable-name))
                  (fkey   (or (gethash id fkey-table)
                              (let ((pg-fkey
                                     (make-pgsql-fkey :table table
                                                      :columns nil
                                                      :foreign-table ftable
                                                      :foreign-columns nil
                                                      :update-rule on-update
                                                      :delete-rule on-delete)))
                                (setf (gethash id fkey-table) pg-fkey)
                                (add-fkey table pg-fkey)
                                pg-fkey))))
             (push-to-end from (pgsql-fkey-columns fkey))
             (push-to-end to   (pgsql-fkey-foreign-columns fkey))))))

(defun list-all-fkeys (schema &key (db *sqlite-db*))
  "Get the list of SQLite foreign keys definitions per table."
  (loop :for table :in (schema-table-list schema)
     :do (list-fkeys table db)))

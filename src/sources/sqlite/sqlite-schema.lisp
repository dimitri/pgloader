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

(defstruct sqlite-idx name table-name sql)

(defmethod index-table-name ((index sqlite-idx))
  (sqlite-idx-table-name index))

(defmethod format-pgsql-create-index ((table table) (index sqlite-idx))
  "Generate the PostgresQL statement to build the given SQLite index definition."
  (sqlite-idx-sql index))

(defun list-all-indexes (schema
                         &key
                           (db *sqlite-db*)
                           including
                           excluding)
  "Get the list of SQLite index definitions per table."
  (let ((sql (format nil
                     "SELECT name, tbl_name, replace(replace(sql, '[', ''), ']', '')
                        FROM sqlite_master
                       WHERE type='index'
                             ~:[~*~;AND (~{~a~^~&~10t or ~})~]
                             ~:[~*~;AND (~{~a~^~&~10t and ~})~]"
                     including          ; do we print the clause?
                     (filter-list-to-where-clause including nil)
                     excluding          ; do we print the clause?
                     (filter-list-to-where-clause excluding t))))
    (log-message :info "~a" sql)
    (loop
       :for (index-name table-name sql) :in (sqlite:execute-to-list db sql)
       :when sql
       :do (let ((table  (find-table schema table-name))
                 (idxdef (make-sqlite-idx :name index-name
                                          :sql sql)))
             (add-index table idxdef))
       :finally (return schema))))


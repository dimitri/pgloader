;;;
;;; SQLite tools connecting to a database
;;;
(in-package :pgloader.sqlite)

(defvar *sqlite-db* nil
  "The SQLite database connection handler.")

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

(defun list-columns (table-name &optional (db *sqlite-db*))
  "Return the list of columns found in TABLE-NAME."
  (let ((sql (format nil "PRAGMA table_info(~a)" table-name)))
    (loop for (seq name type nullable default pk-id) in
	 (sqlite:execute-to-list db sql)
       collect (make-coldef table-name
                            seq
                            name
                            (ctype-to-dtype (normalize type))
                            (normalize type)
                            (= 1 nullable)
                            (unquote default)
                            pk-id))))

(defun list-all-columns (&key
                           (db *sqlite-db*)
                           including
                           excluding)
  "Get the list of SQLite column definitions per table."
  (loop :for table-name :in (list-tables :db db
                                         :including including
                                         :excluding excluding)
     :collect (cons table-name (list-columns table-name db))))

(defstruct sqlite-idx name table-name sql)

(defmethod index-table-name ((index sqlite-idx))
  (sqlite-idx-table-name index))

(defmethod format-pgsql-create-index ((index sqlite-idx))
  "Generate the PostgresQL statement to build the given SQLite index definition."
  (sqlite-idx-sql index))

(defun list-all-indexes (&key
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
    (loop :with schema := nil
       :for (index-name table-name sql) :in (sqlite:execute-to-list db sql)
       :when sql
       :do (let ((entry  (assoc table-name schema :test 'equal))
                 (idxdef (make-sqlite-idx :name index-name
                                          :table-name table-name
                                          :sql sql)))
             (if entry
                 (push-to-end idxdef (cdr entry))
                 (push-to-end (cons table-name (list idxdef)) schema)))
       :finally (return schema))))


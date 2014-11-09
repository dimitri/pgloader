;;;
;;; SQLite tools connecting to a database
;;;
(in-package :pgloader.sqlite)

(defvar *sqlite-db* nil
  "The SQLite database connection handler.")

(defun list-tables (&optional (db *sqlite-db*))
  "Return the list of tables found in SQLITE-DB."
  (let ((sql "SELECT tbl_name
                FROM sqlite_master
               WHERE type='table' AND tbl_name <> 'sqlite_sequence'"))
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

(defun list-all-columns (&optional (db *sqlite-db*))
  "Get the list of SQLite column definitions per table."
  (loop for table-name in (list-tables db)
     collect (cons table-name (list-columns table-name db))))

(defstruct sqlite-idx name table-name sql)

(defmethod index-table-name ((index sqlite-idx))
  (sqlite-idx-table-name index))

(defmethod format-pgsql-create-index ((index sqlite-idx) &key identifier-case)
  "Generate the PostgresQL statement to build the given SQLite index definition."
  (declare (ignore identifier-case))
  (sqlite-idx-sql index))

(defun list-all-indexes (&optional (db *sqlite-db*))
  "Get the list of SQLite index definitions per table."
  (let ((sql "SELECT name, tbl_name, replace(replace(sql, '[', ''), ']', '')
                FROM sqlite_master
               WHERE type='index'"))
    (loop :with schema := nil
       :for (index-name table-name sql) :in (sqlite:execute-to-list db sql)
       :when sql
       :do (let ((entry  (assoc table-name schema :test 'equal))
                 (idxdef (make-sqlite-idx :name index-name
                                          :table-name table-name
                                          :sql sql)))
             (if entry
                 (push idxdef (cdr entry))
                 (push (cons table-name (list idxdef)) schema)))
       :finally (return (reverse (loop for (name . indexes) in schema
                                    collect (cons name (reverse indexes))))))))

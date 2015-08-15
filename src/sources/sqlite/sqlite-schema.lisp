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

(defmethod format-pgsql-create-index ((index sqlite-idx))
  "Generate the PostgresQL statement to build the given SQLite index definition."
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
                 (push-to-end idxdef (cdr entry))
                 (push-to-end (cons table-name (list idxdef)) schema)))
       :finally (return schema))))


;;;
;;; Filtering lists of columns and indexes
;;;
;;; A list of columns is expected to be an alist of table-name associated
;;; with a list of objects (clos or structures) that define the generic API
;;; described in src/pgsql/schema.lisp
;;;
(defun filter-column-list (all-columns &key only-tables including excluding)
  "Apply the filtering defined by the arguments:

    - keep only tables listed in ONLY-TABLES, or all of them if ONLY-TABLES
      is nil,

    - then unless EXCLUDING is nil, filter out the resulting list by
      applying the EXCLUDING regular expression list to table names in the
      all-columns list: we only keep the table names that match none of the
      regex in the EXCLUDING list

    - then unless INCLUDING is nil, only keep remaining elements that
      matches at least one of the INCLUDING regular expression list."

  (labels ((apply-filtering-rule (rule)
	     (declare (special table-name))
	     (typecase rule
	       (string (string-equal rule table-name))
	       (list   (destructuring-bind (type val) rule
			 (ecase type
			   (:regex (cl-ppcre:scan val table-name)))))))

	   (only (entry)
	     (let ((table-name (first entry)))
	       (or (null only-tables)
		   (member table-name only-tables :test #'equal))))

	   (exclude (entry)
	     (let ((table-name (first entry)))
	       (declare (special table-name))
	       (or (null excluding)
		   (notany #'apply-filtering-rule excluding))))

	   (include (entry)
	     (let ((table-name (first entry)))
	       (declare (special table-name))
	       (or (null including)
		   (some #'apply-filtering-rule including)))))

    (remove-if-not #'include
		   (remove-if-not #'exclude
				  (remove-if-not #'only all-columns)))))

;;;
;;; Tools to query the MySQL Schema to reproduce in PostgreSQL
;;;

(in-package :pgloader.source.mysql)

(defclass copy-mysql (db-copy)
  ((encoding :accessor encoding         ; allows forcing encoding
             :initarg :encoding
             :initform nil)
   (range-list :accessor range-list
               :initarg :range-list
               :initform nil))
  (:documentation "pgloader MySQL Data Source"))

;;;
;;; Those functions are to be called from withing an already established
;;; MySQL Connection.
;;;
;;; Tools to get MySQL table and columns definitions and transform them to
;;; PostgreSQL CREATE TABLE statements, and run those.
;;;
(defvar *table-type* '((:table . "BASE TABLE")
		       (:view  . "VIEW"))
  "Associate internal table type symbol with what's found in MySQL
  information_schema.tables.table_type column.")

(defmethod filter-list-to-where-clause ((mysql copy-mysql) filter-list
                                        &key not &allow-other-keys)
  "Given an INCLUDING or EXCLUDING clause, turn it into a MySQL WHERE clause."
  (declare (ignore mysql))
  (mapcar (lambda (filter)
            (typecase filter
              (string-match-rule
               (format nil "~:[~;!~]= '~a'"
                       not
                       (string-match-rule-target filter)))

              (regex-match-rule
               (format nil "~:[~;NOT ~]REGEXP '~a'"
                       not
                       (regex-match-rule-target filter)))))
          filter-list))

(defun cleanup-default-value (dtype default)
  "MySQL catalog query always returns the default value as a string, but in
   the case of a binary data type we actually want a byte vector."
  (cond ((string= "binary" dtype)
         (when default
           (babel:string-to-octets default)))

        (t (ensure-unquoted default #\'))))

(defmethod fetch-columns ((schema schema)
                          (mysql copy-mysql)
                          &key
                            (table-type :table)
                            including
                            excluding
                          &aux
                            (table-type-name
                             (cdr (assoc table-type *table-type*))))
  "Get the list of MySQL column names per table."
  (loop
     :for (tname tcomment cname ccomment dtype ctype default nullable extra)
     :in (mysql-query (sql "/mysql/list-all-columns.sql"
                           (db-name *connection*)
                           table-type-name
                           including    ; do we print the clause?
                           including
                           excluding    ; do we print the clause?
                           excluding))
     :do
     (let* ((table
             (case table-type
               (:view (maybe-add-view schema tname :comment tcomment))
               (:table (maybe-add-table schema tname :comment tcomment))))
            (def-val (cleanup-default-value dtype default))
            (field   (make-mysql-column
                      tname cname (unless (or (null ccomment)
                                              (string= "" ccomment))
                                    ccomment)
                      dtype ctype def-val nullable
                      (normalize-extra extra))))
       (add-field table field))
     :finally
     (return schema)))

(defmethod fetch-indexes ((schema schema) (mysql copy-mysql)
                          &key including excluding)
  "Get the list of MySQL index definitions per table."
  (loop
     :for (table-name name index-type non-unique cols)
     :in (mysql-query (sql "/mysql/list-all-indexes.sql"
                           (db-name *connection*)
                           including ; do we print the clause?
                           including
                           excluding ; do we print the clause?
                           excluding))
     :do (let* ((table (find-table schema table-name))
                (index
                 (make-index :name name ; further processing is needed
                             :schema schema
                             :table table
                             :type index-type
                             :primary (string= name "PRIMARY")
                             :unique (string= "0" non-unique)
                             :columns (mapcar
                                       #'apply-identifier-case
                                       (sq:split-sequence #\, cols)))))
           (add-index table index))
     :finally
     (return schema)))

;;;
;;; MySQL Foreign Keys
;;;
(defmethod fetch-foreign-keys ((schema schema)
                               (mysql copy-mysql)
                               &key
                                 including
                                 excluding)
  "Get the list of MySQL Foreign Keys definitions per table."
  (loop
     :for (table-name name ftable-name cols fcols update-rule delete-rule)
     :in (mysql-query (sql "/mysql/list-all-fkeys.sql"
                           (db-name *connection*) (db-name *connection*)
                           including    ; do we print the clause?
                           including
                           excluding    ; do we print the clause?
                           excluding))
     :do (let* ((table  (find-table schema table-name))
                (ftable (find-table schema ftable-name))
                (fk
                 (make-fkey :name (apply-identifier-case name)
                            :table table
                            :columns (mapcar #'apply-identifier-case
                                             (sq:split-sequence #\, cols))
                            :foreign-table ftable
                            :foreign-columns (mapcar
                                              #'apply-identifier-case
                                              (sq:split-sequence #\, fcols))
                            :update-rule update-rule
                            :delete-rule delete-rule)))
           (if (and name table ftable)
               (add-fkey table fk)
               ;; chances are this comes from the EXCLUDING clause, but
               ;; we'll make for it in fetching missing dependencies for
               ;; (unique) indexes
               (log-message :info
                            "Incomplete Foreign Key definition: constraint ~s on table ~s referencing table ~s"
                            name
                            (when table (format-table-name table))
                            (when ftable (format-table-name ftable)))))
     :finally
     (return schema)))

;;;
;;; MySQL table row count estimate
;;;
(defmethod fetch-table-row-count ((schema schema)
                                  (mysql copy-mysql)
                                  &key
                                    including
                                    excluding)
  "Retrieve and set the row count estimate for given MySQL tables."
  (loop
     :for (table-name count)
     :in (mysql-query (sql "/mysql/list-table-rows.sql"
                           (db-name *connection*)
                           including    ; do we print the clause?
                           including
                           excluding    ; do we print the clause?
                           excluding))
     :do (let* ((table  (find-table schema table-name)))
           (when table
             (setf (table-row-count-estimate table) (parse-integer count))))))


;;;
;;; Queries to get the MySQL comments.
;;;
;;; As it takes a separate PostgreSQL Query per comment it's useless to
;;; fetch them right into the the more general table and columns lists.
;;;
(defun list-table-comments (&key including excluding)
  "Return comments on MySQL tables."
  (loop
     :for (table-name comment)
     :in (mysql-query (sql "/mysql/list-table-comments.sql"
                           (db-name *connection*)
                           including ; do we print the clause?
                           including
                           excluding ; do we print the clause?
                           excluding))
     :when (and comment (not (string= comment "")))
     :collect (list table-name comment)))

(defun list-columns-comments (&key including excluding)
  "Return comments on MySQL tables."
  (loop
     :for (table-name column-name comment)
     :in (mysql-query (sql "/mysql/list-columns-comments.sql"
                           (db-name *connection*)
                           including    ; do we print the clause?
                           including
                           excluding    ; do we print the clause?
                           excluding))
     :when (and comment (not (string= comment "")))
     :collect (list table-name column-name comment)))


;;;
;;; Tools to handle row queries, issuing separate is null statements and
;;; handling of geometric data types.
;;;
(defmethod get-column-sql-expression ((mysql copy-mysql) name type)
  "Return per-TYPE SQL expression to use given a column NAME.

   Mostly we just use the name, but in case of POINT we need to use
   st_astext(name)."
  (declare (ignore mysql))
  (case (intern (string-upcase type) "KEYWORD")
    (:geometry   (format nil "st_astext(`~a`) as `~a`" name name))
    (:point      (format nil "st_astext(`~a`) as `~a`" name name))
    (:linestring (format nil "st_astext(`~a`) as `~a`" name name))
    (t           (format nil "`~a`" name))))

(defmethod get-column-list ((mysql copy-mysql))
  "Some MySQL datatypes have a meaningless default output representation, we
   need to process them on the SQL side (geometric data types)."
  (loop :for field :in (fields mysql)
     :collect (let ((name (mysql-column-name field))
                    (type (mysql-column-dtype field)))
                (get-column-sql-expression mysql name type))))

;;;
;;; Tools to query the MySQL Schema to reproduce in PostgreSQL
;;;

(in-package :pgloader.source.mysql)

;;;
;;; Those functions are to be called from withing an already established
;;; MySQL Connection.
;;;
;;; Handle MATERIALIZE VIEWS sections, where we need to create the views in
;;; the MySQL database before being able to process them.
;;;
(defun create-my-views (views-alist)
  "VIEWS-ALIST associates view names with their SQL definition, which might
   be empty for already existing views. Create only the views for which we
   have an SQL definition."
  (unless (eq :all views-alist)
   (let ((views (remove-if #'null views-alist :key #'cdr)))
     (when views
       (loop for (name . def) in views
          for sql = (format nil "CREATE VIEW ~a AS ~a" name def)
          do
            (log-message :info "MySQL: ~a" sql)
            #+pgloader-image
            (mysql-query sql)
            #-pgloader-image
            (restart-case
                (mysql-query sql)
              (use-existing-view ()
                :report "Use the already existing view and continue"
                nil)
              (replace-view ()
                :report "Replace the view with the one from pgloader's command"
                (let ((drop-sql (format nil "DROP VIEW ~a;" name)))
                  (log-message :info "MySQL: ~a" drop-sql)
                  (mysql-query drop-sql)
                  (mysql-query sql)))))))))

(defun drop-my-views (views-alist)
  "See `create-my-views' for VIEWS-ALIST description. This time we DROP the
   views to clean out after our work."
  (unless (eq :all views-alist)
   (let ((views (remove-if #'null views-alist :key #'cdr)))
     (when views
       (let ((sql
              (format nil "DROP VIEW ~{~a~^, ~};" (mapcar #'car views))))
         (log-message :info "MySQL: ~a" sql)
         (mysql-query sql))))))


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

(defun filter-list-to-where-clause (filter-list &optional not)
  "Given an INCLUDING or EXCLUDING clause, turn it into a MySQL WHERE clause."
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

(defun list-all-columns (schema
                         &key
			   (table-type :table)
			   only-tables
                           including
                           excluding
			 &aux
			   (table-type-name (cdr (assoc table-type *table-type*))))
  "Get the list of MySQL column names per table."
  (loop
     :for (tname tcomment cname ccomment dtype ctype default nullable extra)
     :in
     (mysql-query (format nil
                          (sql "/mysql/list-all-columns.sql")
                          (db-name *connection*)
                          table-type-name
                          only-tables   ; do we print the clause?
                          only-tables
                          including     ; do we print the clause?
                          (filter-list-to-where-clause including)
                          excluding     ; do we print the clause?
                          (filter-list-to-where-clause excluding t)))
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

(defun list-all-indexes (schema
                         &key
                           only-tables
                           including
                           excluding)
  "Get the list of MySQL index definitions per table."
  (loop
     :for (table-name name index-type non-unique cols)
     :in (mysql-query (format nil
                              (sql "/mysql/list-all-indexes.sql")
                              (db-name *connection*)
                              only-tables ; do we print the clause?
                              only-tables
                              including ; do we print the clause?
                              (filter-list-to-where-clause including)
                              excluding ; do we print the clause?
                              (filter-list-to-where-clause excluding t)))
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
(defun list-all-fkeys (schema
                       &key
                         only-tables
                         including
                         excluding)
  "Get the list of MySQL Foreign Keys definitions per table."
  (loop
     :for (table-name name ftable-name cols fcols update-rule delete-rule)
     :in (mysql-query (format nil
                              (sql "/mysql/list-all-fkeys.sql")
                              (db-name *connection*) (db-name *connection*)
                              only-tables ; do we print the clause?
                              only-tables
                              including ; do we print the clause?
                              (filter-list-to-where-clause including)
                              excluding ; do we print the clause?
                              (filter-list-to-where-clause excluding t)))
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
;;; Queries to get the MySQL comments.
;;;
;;; As it takes a separate PostgreSQL Query per comment it's useless to
;;; fetch them right into the the more general table and columns lists.
;;;
(defun list-table-comments (&key
                              only-tables
                              including
                              excluding)
  "Return comments on MySQL tables."
  (loop
     :for (table-name comment)
     :in (mysql-query (format nil
                              (sql "/mysql/list-table-comments.sql")
                              (db-name *connection*)
                              only-tables ; do we print the clause?
                              only-tables
                              including ; do we print the clause?
                              (filter-list-to-where-clause including)
                              excluding ; do we print the clause?
                              (filter-list-to-where-clause excluding t)))
     :when (and comment (not (string= comment "")))
     :collect (list table-name comment)))

(defun list-columns-comments (&key
                                only-tables
                                including
                                excluding)
  "Return comments on MySQL tables."
  (loop
     :for (table-name column-name comment)
     :in (mysql-query (format nil
                              (sql "/mysql/list-columns-comments.sql")
                              (db-name *connection*)
                              only-tables ; do we print the clause?
                              only-tables
                              including ; do we print the clause?
                              (filter-list-to-where-clause including)
                              excluding ; do we print the clause?
                              (filter-list-to-where-clause excluding t)))
     :when (and comment (not (string= comment "")))
     :collect (list table-name column-name comment)))


;;;
;;; Tools to handle row queries, issuing separate is null statements and
;;; handling of geometric data types.
;;;
(defun get-column-sql-expression (name type)
  "Return per-TYPE SQL expression to use given a column NAME.

   Mostly we just use the name, but in case of POINT we need to use
   astext(name)."
  (case (intern (string-upcase type) "KEYWORD")
    (:geometry   (format nil "astext(`~a`) as `~a`" name name))
    (:point      (format nil "astext(`~a`) as `~a`" name name))
    (:linestring (format nil "astext(`~a`) as `~a`" name name))
    (t           (format nil "`~a`" name))))

(defun get-column-list (copy)
  "Some MySQL datatypes have a meaningless default output representation, we
   need to process them on the SQL side (geometric data types)."
  (loop :for field :in (fields copy)
     :collect (get-column-sql-expression (mysql-column-name field)
                                         (mysql-column-dtype field))))

(declaim (inline fix-nulls))

(defun fix-nulls (row nulls)
  "Given a cl-mysql row result and a nulls list as from
   get-column-list-with-is-nulls, replace NIL with empty strings with when
   we know from the added 'foo is null' that the actual value IS NOT NULL.

   See http://bugs.mysql.com/bug.php?id=19564 for context."
  (loop
     for (current-col  next-col)  on row
     for (current-null next-null) on nulls
     ;; next-null tells us if next column is an "is-null" col
     ;; when next-null is true, next-col is true if current-col is actually null
     for is-null = (and next-null (string= next-col "1"))
     for is-empty = (and next-null (string= next-col "0") (null current-col))
     ;; don't collect columns we added, e.g. "column_name is not null"
     when (not current-null)
     collect (cond (is-null :null)
		   (is-empty "")
		   (t current-col))))


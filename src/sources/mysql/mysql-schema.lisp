;;;
;;; Tools to query the MySQL Schema to reproduce in PostgreSQL
;;;

(in-package :pgloader.mysql)

(defvar *connection* nil "Current MySQL connection")


;;;
;;; General utility to manage MySQL connection
;;;
(defclass mysql-connection (db-connection) ())

(defmethod initialize-instance :after ((myconn mysql-connection) &key)
  "Assign the type slot to mysql."
  (setf (slot-value myconn 'type) "mysql"))

(defmethod open-connection ((myconn mysql-connection) &key)
  (setf (conn-handle myconn)
        (if (and (consp (db-host myconn)) (eq :unix (car (db-host myconn))))
            (qmynd:mysql-local-connect :path (cdr (db-host myconn))
                                       :username (db-user myconn)
                                       :password (db-pass myconn)
                                       :database (db-name myconn))
            (qmynd:mysql-connect :host (db-host myconn)
                                 :port (db-port myconn)
                                 :username (db-user myconn)
                                 :password (db-pass myconn)
                                 :database (db-name myconn))))
  (log-message :debug "CONNECTED TO ~a" myconn)
  ;; return the connection object
  myconn)

(defmethod close-connection ((myconn mysql-connection))
  (qmynd:mysql-disconnect (conn-handle myconn))
  (setf (conn-handle myconn) nil)
  myconn)

(defmethod clone-connection ((c mysql-connection))
  (change-class (call-next-method c) 'mysql-connection))

(defmethod query ((myconn mysql-connection)
                  sql
                  &key
                    row-fn
                    (as-text t)
                    (result-type 'list))
  "Run SQL query against MySQL connection MYCONN."
  (log-message :debug "MySQL: sending query: ~a" sql)
  (qmynd:mysql-query (conn-handle myconn)
                     sql
                     :row-fn row-fn
                     :as-text as-text
                     :result-type result-type))

;;;
;;; The generic API query is recent, used to look like this:
;;;
(defun mysql-query (query &key row-fn (as-text t) (result-type 'list))
  "Execute given QUERY within the current *connection*, and set proper
   defaults for pgloader."
  (log-message :debug "MySQL: sending query: ~a" query)
  (qmynd:mysql-query (conn-handle *connection*) query
                     :row-fn row-fn
                     :as-text as-text
                     :result-type result-type))

;;;
;;; Function for accessing the MySQL catalogs, implementing auto-discovery.
;;;
;;; Interactive use only, will create its own database connection.
;;;
;; (defun list-databases ()
;;   "Connect to a local database and get the database list"
;;   (with-mysql-connection ()
;;     (mysql-query "show databases")))

;; (defun list-tables (dbname)
;;   "Return a flat list of all the tables names known in given DATABASE"
;;   (with-mysql-connection (dbname)
;;     (mysql-query (format nil "
;;   select table_name
;;     from information_schema.tables
;;    where table_schema = '~a' and table_type = 'BASE TABLE'
;; order by table_name" dbname))))

;; (defun list-views (dbname &key only-tables)
;;   "Return a flat list of all the view names and definitions known in given DBNAME"
;;   (with-mysql-connection (dbname)
;;     (mysql-query (format nil "
;;   select table_name, view_definition
;;     from information_schema.views
;;    where table_schema = '~a'
;;          ~@[and table_name in (~{'~a'~^,~})~]
;; order by table_name" dbname only-tables))))


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
            (mysql-query sql))))))

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
              (string (format nil "~:[~;!~]= '~a'" not filter))
              (cons   (format nil "~:[~;NOT ~]REGEXP '~a'" not (cadr filter)))))
          filter-list))

(defun cleanup-default-value (dtype default)
  "MySQL catalog query always returns the default value as a string, but in
   the case of a binary data type we actually want a byte vector."
  (cond ((string= "binary" dtype)
         (when default
           (babel:string-to-octets default)))

        (t default)))

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
     (mysql-query (format nil "
  select c.table_name, t.table_comment,
         c.column_name, c.column_comment,
         c.data_type, c.column_type, c.column_default,
         c.is_nullable, c.extra
    from information_schema.columns c
         join information_schema.tables t on binary t.table_schema=c.table_schema and binary t.table_name=c.table_name
   where c.table_schema = '~a' and t.table_type = '~a'
         ~:[~*~;and table_name in (~{'~a'~^,~})~]
         ~:[~*~;and (~{table_name ~a~^ or ~})~]
         ~:[~*~;and (~{table_name ~a~^ and ~})~]
order by table_name, ordinal_position"
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
                      dtype ctype def-val nullable extra)))
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
     :for (table-name name non-unique cols)
     :in (mysql-query (format nil "
  SELECT table_name, index_name, sum(non_unique),
         cast(GROUP_CONCAT(column_name order by seq_in_index) as char)
    FROM information_schema.statistics
   WHERE table_schema = '~a'
         ~:[~*~;and table_name in (~{'~a'~^,~})~]
         ~:[~*~;and (~{table_name ~a~^ or ~})~]
         ~:[~*~;and (~{table_name ~a~^ and ~})~]
GROUP BY table_name, index_name;"
                             (db-name *connection*)
                             only-tables ; do we print the clause?
                             only-tables
                             including  ; do we print the clause?
                             (filter-list-to-where-clause including)
                             excluding  ; do we print the clause?
                             (filter-list-to-where-clause excluding t)))
     :do (let ((table (find-table schema table-name))
               (index
                (make-pgsql-index :name name ; further processing is needed
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
     :in (mysql-query (format nil "
SELECT s.table_name, s.constraint_name, s.ft, s.cols, s.fcols,
       rc.update_rule, rc.delete_rule

FROM
 (
  SELECT tc.table_schema, tc.table_name,
         tc.constraint_name, k.referenced_table_name ft,

             group_concat(         k.column_name
                          order by k.ordinal_position) as cols,

             group_concat(         k.referenced_column_name
                          order by k.position_in_unique_constraint) as fcols

        FROM information_schema.table_constraints tc

        LEFT JOIN information_schema.key_column_usage k
               ON k.table_schema = tc.table_schema
              AND k.table_name = tc.table_name
              AND k.constraint_name = tc.constraint_name

      WHERE     tc.table_schema = '~a'
            AND k.referenced_table_schema = '~a'
            AND tc.constraint_type = 'FOREIGN KEY'
           ~:[~*~;and tc.table_name in (~{'~a'~^,~})~]
           ~:[~*~;and (~{tc.table_name ~a~^ or ~})~]
           ~:[~*~;and (~{tc.table_name ~a~^ and ~})~]

   GROUP BY tc.table_schema, tc.table_name, tc.constraint_name, ft
 ) s
             JOIN information_schema.referential_constraints rc
               ON rc.constraint_schema = s.table_schema
              AND rc.constraint_name = s.constraint_name
              AND rc.table_name = s.table_name"
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
                 (make-pgsql-fkey :name (apply-identifier-case name)
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
               (log-message :error
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
     :in (mysql-query (format nil "
    SELECT table_name, table_comment
      FROM information_schema.tables
    WHERE     table_schema = '~a'
          and table_type = 'BASE TABLE'
         ~:[~*~;and table_name in (~{'~a'~^,~})~]
         ~:[~*~;and (~{table_name ~a~^ or ~})~]
         ~:[~*~;and (~{table_name ~a~^ and ~})~]"
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
     :in (mysql-query (format nil "
  select c.table_name, c.column_name, c.column_comment
    from information_schema.columns c
         join information_schema.tables t using(table_schema, table_name)
   where     c.table_schema = '~a'
         and t.table_type = 'BASE TABLE'
         ~:[~*~;and table_name in (~{'~a'~^,~})~]
         ~:[~*~;and (~{table_name ~a~^ or ~})~]
         ~:[~*~;and (~{table_name ~a~^ and ~})~]
order by table_name, ordinal_position"
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
    (:point  (format nil "astext(`~a`) as `~a`" name name))
    (t       (format nil "`~a`" name))))

(defun get-column-list (dbname table-name)
  "Some MySQL datatypes have a meaningless default output representation, we
   need to process them on the SQL side (geometric data types).

   This function assumes a valid connection to the MySQL server has been
   established already."
  (loop
     for (name type) in (mysql-query (format nil "
  select column_name, data_type
    from information_schema.columns
   where table_schema = '~a' and table_name = '~a'
order by ordinal_position" dbname table-name)
					   :result-type 'list)
     collect (get-column-sql-expression name type)))

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


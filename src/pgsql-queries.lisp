;;;
;;; Tools to handle PostgreSQL queries
;;;
(in-package :pgloader.pgsql)

;;;
;;; PostgreSQL Tools connecting to a database
;;;
(defmacro with-pgsql-transaction ((dbname &key database) &body forms)
  "Run FORMS within a PostgreSQL transaction to DBNAME, reusing DATABASE if
   given. To get the connection spec from the DBNAME, use `get-connection-spec'."
  (if database
      `(let ((pomo:*database* database))
	 (pomo:with-transaction ()
	   (set-session-gucs *pg-settings* :transaction t)
	   (progn ,@forms)))
      ;; no database given, create a new database connection
      `(pomo:with-connection (get-connection-spec ,dbname)
	 (set-session-gucs *pg-settings*)
	 (pomo:with-transaction ()
	   (progn ,@forms)))))

(defun get-connection-spec (dbname &key (with-port t))
  "pomo:with-connection and cl-postgres:open-database and open-db-writer are
   not using the same connection spec format..."
  (let ((conspec (list dbname *pgconn-user* *pgconn-pass* *pgconn-host*)))
    (if with-port
      (append conspec (list :port *pgconn-port*))
      (append conspec (list *pgconn-port*)))))

(defun set-session-gucs (alist &key transaction database)
  "Set given GUCs to given values for the current session."
  (let ((pomo:*database* (or database pomo:*database*)))
    (loop
       for (name . value) in alist
       for set = (format nil "SET~@[ LOCAL~] ~a TO '~a'" transaction name value)
       do
	 (log-message :debug set)
	 (pomo:execute set))))

;;;
;;; PostgreSQL queries
;;;
(defun pgsql-execute (sql &key ((:client-min-messages level)))
  "Execute given SQL in current transaction"
  (when level
    (pomo:execute
     (format nil "SET LOCAL client_min_messages TO ~a;" (symbol-name level))))

  (pomo:execute sql)

  (when level (pomo:execute (format nil "RESET client_min_messages;"))))

;;;
;;; PostgreSQL Utility Queries
;;;
(defun truncate-table (dbname table-name)
  "Truncate given TABLE-NAME in database DBNAME"
  (pomo:with-connection (get-connection-spec dbname)
    (pomo:execute (format nil "truncate ~a;" table-name))))

(defun list-databases (&optional (username "postgres"))
  "Connect to a local database and get the database list"
  (pomo:with-connection (let ((*pgconn-user* username))
			  (get-connection-spec "postgres"))
    (loop for (dbname) in (pomo:query
			   "select datname
                              from pg_database
                             where datname !~ 'postgres|template'")
       collect dbname)))

(defun list-tables (dbname)
  "Return an alist of tables names and list of columns to pay attention to."
  (pomo:with-connection
      (get-connection-spec dbname)

    (loop for (relname colarray) in (pomo:query "
select relname, array_agg(case when typname in ('date', 'timestamptz')
                               then attnum end
                          order by attnum)
      from pg_class c
           join pg_namespace n on n.oid = c.relnamespace
           left join pg_attribute a on c.oid = a.attrelid
           join pg_type t on t.oid = a.atttypid
     where c.relkind = 'r'
           and attnum > 0
           and n.nspname = 'public'
  group by relname
")
       collect (cons relname (loop
				for attnum across colarray
				unless (eq attnum :NULL)
				collect attnum)))))

(defun list-tables-cols (dbname &key (schema "public") table-name-list)
  "Return an alist of tables names and number of columns."
  (pomo:with-connection
      (get-connection-spec dbname)

    (loop for (relname cols)
       in (pomo:query (format nil "
    select relname, count(attnum)
      from pg_class c
           join pg_namespace n on n.oid = c.relnamespace
           left join pg_attribute a on c.oid = a.attrelid
           join pg_type t on t.oid = a.atttypid
     where c.relkind = 'r'
           and attnum > 0
           and n.nspname = '~a'
           ~@[~{and relname = '~a'~^ ~}~]
  group by relname
" schema table-name-list))
       collect (cons relname cols))))

(defun list-columns (dbname table-name &key schema)
  "Return a list of column names for given TABLE-NAME."
  (pomo:with-connection
      (get-connection-spec dbname)

    (pomo:query (format nil "
    select attname
      from pg_class c
           join pg_namespace n on n.oid = c.relnamespace
           left join pg_attribute a on c.oid = a.attrelid
           join pg_type t on t.oid = a.atttypid
     where c.oid = '~:[~*~a~;~a.~a~]'::regclass and attnum > 0
  order by attnum" schema schema table-name) :column)))

(defun list-reserved-keywords (dbname)
  "Connect to PostgreSQL DBNAME and fetch reserved keywords."
  (with-pgsql-transaction (dbname)
    (pomo:query "select word from pg_get_keywords() where catcode = 'R'" :column)))

(defun reset-all-sequences (dbname &key only-tables)
  "Reset all sequences to the max value of the column they are attached to."
  (pomo:with-connection (get-connection-spec dbname)
    (pomo:execute "set client_min_messages to warning;")
    (pomo:execute "listen seqs")

    (handler-case
	(pomo:execute (format nil "
DO $$
DECLARE
  n integer := 0;
  r record;
BEGIN
  FOR r in
       SELECT 'select '
               || trim(trailing ')'
                  from replace(pg_get_expr(d.adbin, d.adrelid),
                               'nextval', 'setval'))
               || ', (select greatest(max(' || a.attname || '), 1) from only '
               || nspname || '.' || relname || '));' as sql
         FROM pg_class c
              JOIN pg_namespace n on n.oid = c.relnamespace
              JOIN pg_attribute a on a.attrelid = c.oid
              JOIN pg_attrdef d on d.adrelid = a.attrelid
                                 and d.adnum = a.attnum
                                 and a.atthasdef
        WHERE relkind = 'r' and a.attnum > 0
              and pg_get_expr(d.adbin, d.adrelid) ~~ '^nextval'
              ~@[and c.oid in (~{'~a'::regclass~^, ~})~]
  LOOP
    n := n + 1;
    EXECUTE r.sql;
  END LOOP;

  PERFORM pg_notify('seqs', n::text);
END;
$$; " only-tables))
      ;; now get the notification signal
      (cl-postgres:postgresql-notification (c)
	(parse-integer (cl-postgres:postgresql-notification-payload c))))))

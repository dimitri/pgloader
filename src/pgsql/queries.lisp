;;;
;;; Tools to handle PostgreSQL queries
;;;
(in-package :pgloader.pgsql)

;;;
;;; PostgreSQL Tools connecting to a database
;;;
(defmacro handling-pgsql-notices (&body forms)
  "The BODY is run within a PostgreSQL transaction where *pg-settings* have
   been applied. PostgreSQL warnings and errors are logged at the
   appropriate log level."
  `(handler-bind
       ((cl-postgres:database-error
	 #'(lambda (e)
	     (log-message :error "~a" e)))
	(cl-postgres:postgresql-warning
	 #'(lambda (w)
	     (log-message :warning "~a" w)
	     (muffle-warning))))
     (progn ,@forms)))

(defmacro with-pgsql-transaction ((&key dbname username database) &body forms)
  "Run FORMS within a PostgreSQL transaction to DBNAME, reusing DATABASE if
   given. To get the connection spec from the DBNAME, use `get-connection-spec'."
  (if database
      `(let ((pomo:*database* ,database))
	 (handling-pgsql-notices
               (pomo:with-transaction ()
                 (log-message :debug "BEGIN")
                 (set-session-gucs *pg-settings* :transaction t)
                 ,@forms)))
      ;; no database given, create a new database connection
      `(let (#+unix (cl-postgres::*unix-socket-dir* (get-unix-socket-dir)))
	 (pomo:with-connection (get-connection-spec :dbname ,dbname
                                                    :username ,username)
	   (log-message :debug "CONNECT")
	   (set-session-gucs *pg-settings*)
	   (handling-pgsql-notices ()
               (pomo:with-transaction ()
                 (log-message :debug "BEGIN")
                 ,@forms))))))

(defmacro with-pgsql-connection ((dbname) &body forms)
  "Run FROMS within a PostgreSQL connection to DBNAME. To get the connection
   spec from the DBNAME, use `get-connection-spec'."
  `(let (#+unix (cl-postgres::*unix-socket-dir*  (get-unix-socket-dir)))
     (pomo:with-connection (get-connection-spec :dbname ,dbname)
       (log-message :debug "CONNECT ~s" (get-connection-spec :dbname ,dbname))
       (set-session-gucs *pg-settings*)
       (handling-pgsql-notices ()
           ,@forms))))

(defun get-unix-socket-dir ()
  "When *pgconn* host is a (cons :unix path) value, return the right value
   for cl-postgres::*unix-socket-dir*."
  (destructuring-bind (&key host &allow-other-keys) *pgconn*
    (if (and (consp host) (eq :unix (car host)))
        ;; set to *pgconn* host value
        (directory-namestring (fad:pathname-as-directory (cdr host)))
        ;; keep as is.
        cl-postgres::*unix-socket-dir*)))

(defun get-connection-spec (&key dbname username (with-port t))
  "pomo:with-connection and cl-postgres:open-database and open-db-writer are
   not using the same connection spec format..."
  (destructuring-bind (&key type host port user password
                            ((:dbname pgconn-dbname))
                            use-ssl
                            table-name)
      *pgconn*
    (declare (ignore type table-name))
    (let* ((host    (if (and (consp host) (eq :unix (car host))) :unix host))
           (conspec (list (or dbname pgconn-dbname)
                          (or username user)
                          password
                          host)))
      (if with-port
          (setf conspec (append conspec (list :port port)))
          (setf conspec (append conspec (list port))))

      (if use-ssl (append conspec (list :use-ssl use-ssl)) conspec))))

(defun set-session-gucs (alist &key transaction database)
  "Set given GUCs to given values for the current session."
  (let ((pomo:*database* (or database pomo:*database*)))
    (loop
       for (name . value) in alist
       for set = (format nil "SET~:[~; LOCAL~] ~a TO '~a'" transaction name value)
       do
	 (log-message :debug set)
	 (pomo:execute set))))

(defun pgsql-execute-with-timing (dbname label sql state &key (count 1))
  "Execute given SQL and resgister its timing into STATE."
  (multiple-value-bind (res secs)
      (timing
       (handler-case
	   (with-pgsql-transaction (:dbname dbname)
	     (pgsql-execute sql))
	 (cl-postgres:database-error (e)
	   (declare (ignore e))		; a log has already been printed
	   (pgstate-incf state label :errs 1 :rows (- count)))))
    (declare (ignore res))
    (pgstate-incf state label :read count :rows count :secs secs)))

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

(defun list-databases (&optional (username "postgres"))
  "Connect to a local database and get the database list"
  (with-pgsql-transaction (:dbname "postgres" :username username)
    (loop for (dbname) in (pomo:query
                           "select datname
                              from pg_database
                             where datname !~ 'postgres|template'")
       collect dbname)))

(defun list-tables (&optional dbname)
  "Return an alist of tables names and list of columns to pay attention to."
  (with-pgsql-transaction (:dbname dbname)
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

(defun list-tables-and-fkeys (&optional schema)
  "Yet another table listing query."
  (loop for (relname fkeys) in (pomo:query (format nil "
  select relname, array_to_string(array_agg(conname), ',')
    from pg_class c
         join pg_namespace n on n.oid = c.relnamespace
         left join pg_constraint co on c.oid = co.conrelid
    where contype = 'f' and nspname = ~:[current_schema()~;'~a'~]
 group by relname;" schema schema))
     collect (cons relname (sq:split-sequence #\, fkeys))))

(defun list-columns (dbname table-name &key schema)
  "Return a list of column names for given TABLE-NAME."
  (with-pgsql-transaction (:dbname dbname)
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
  (with-pgsql-transaction (:dbname dbname)
    (pomo:query "select word
                   from pg_get_keywords()
                  where catcode IN ('R', 'T')" :column)))

(defun reset-all-sequences (dbname &key tables)
  "Reset all sequences to the max value of the column they are attached to."
  (with-pgsql-connection (dbname)
    (set-session-gucs *pg-settings*)
    (pomo:execute "set client_min_messages to warning;")
    (pomo:execute "listen seqs")

    (when tables
      (pomo:execute
       (format nil "create temp table reloids(oid) as values ~{('~a'::regclass)~^,~}"
	       tables)))

    (handler-case
	(let ((sql (format nil "
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
               || quote_ident(nspname) || '.' || quote_ident(relname) || '));' as sql
         FROM pg_class c
              JOIN pg_namespace n on n.oid = c.relnamespace
              JOIN pg_attribute a on a.attrelid = c.oid
              JOIN pg_attrdef d on d.adrelid = a.attrelid
                                 and d.adnum = a.attnum
                                 and a.atthasdef
        WHERE relkind = 'r' and a.attnum > 0
              and pg_get_expr(d.adbin, d.adrelid) ~~ '^nextval'
              ~@[and c.oid in (select oid from reloids)~]
  LOOP
    n := n + 1;
    EXECUTE r.sql;
  END LOOP;

  PERFORM pg_notify('seqs', n::text);
END;
$$; " tables)))
	  (pomo:execute sql))
      ;; now get the notification signal
      (cl-postgres:postgresql-notification (c)
	(parse-integer (cl-postgres:postgresql-notification-payload c))))))

(defun list-table-oids (table-names)
  "Return an alist of (TABLE-NAME . TABLE-OID) for all table in the
   TABLE-NAMES list. A connection must be established already."
  (when table-names
    (loop for (name oid)
       in (pomo:query
	   (format nil
		   "select n, n::regclass::oid from (values ~{('~a')~^,~}) as t(n)"
		   table-names))
       collect (cons name oid))))

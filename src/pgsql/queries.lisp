;;;
;;; Tools to handle PostgreSQL queries
;;;
(in-package :pgloader.pgsql)

(defun list-schemas ()
  "Return the list of PostgreSQL schemas in the already established
   PostgreSQL connection."
  (pomo:query "SELECT nspname FROM pg_catalog.pg_namespace;" :column))

(defun list-tables-and-fkeys (&optional schema-name)
  "Yet another table listing query."
  (loop :for (relname fkeys) :in (pomo:query (format nil "
  select relname, array_to_string(array_agg(conname), ',')
    from pg_class c
         join pg_namespace n on n.oid = c.relnamespace
         left join pg_constraint co on c.oid = co.conrelid
    where contype = 'f' and nspname = ~:[current_schema()~;'~a'~]
 group by relname;" schema-name schema-name))
     :collect (cons relname (sq:split-sequence #\, fkeys))))

(defun list-columns-query (table-name &optional schema)
  "Returns the list of columns for table TABLE-NAME in schema SCHEMA, and
   must be run with an already established PostgreSQL connection."
  (pomo:query (format nil "
    select attname, t.oid::regtype
      from pg_class c
           join pg_namespace n on n.oid = c.relnamespace
           left join pg_attribute a on c.oid = a.attrelid
           join pg_type t on t.oid = a.atttypid
     where c.oid = '~:[~*~a~;~a.~a~]'::regclass and attnum > 0
  order by attnum" schema schema table-name)))

(defun list-columns (pgconn table-name &key schema)
  "Return a list of column names for given TABLE-NAME."
  (with-pgsql-transaction (:pgconn pgconn)
    (with-schema (unqualified-table-name table-name)
      (loop :for (name type)
         :in (list-columns-query unqualified-table-name schema)
         :collect name))))

(defun list-indexes (table)
  "List all indexes for TABLE-NAME in SCHEMA. A PostgreSQL connection must
   be already established when calling that function."
  (loop
     :with sql-index-list
     := (let ((sql (format nil "
select i.relname,
       n.nspname,
       indrelid::regclass,
       indrelid,
       indisprimary,
       indisunique,
       pg_get_indexdef(indexrelid),
       c.conname,
       pg_get_constraintdef(c.oid)
  from pg_index x
       join pg_class i ON i.oid = x.indexrelid
       join pg_namespace n ON n.oid = i.relnamespace
       left join pg_constraint c ON c.conindid = i.oid
 where indrelid = '~a'::regclass"
                           (format-table-name table))))
          (log-message :debug "~a" sql)
          sql)

     :for (name schema table-name table-oid primary unique sql conname condef)
     :in (pomo:query sql-index-list)

     :collect (make-pgsql-index :name name
                                :schema schema
                                :table-oid table-oid
                                :primary primary
                                :unique unique
                                :columns nil
                                :sql sql
                                :conname (unless (eq :null conname) conname)
                                :condef  (unless (eq :null condef)  condef))))

(defun reset-all-sequences (pgconn &key tables)
  "Reset all sequences to the max value of the column they are attached to."
  (let ((newconn (clone-connection pgconn)))
    (with-pgsql-connection (newconn)
      (set-session-gucs *pg-settings*)
      (pomo:execute "set client_min_messages to warning;")
      (pomo:execute "listen seqs")

      (when tables
        (pomo:execute
         (format nil "create temp table reloids(oid) as values ~{('~a'::regclass)~^,~}"
                 (mapcar #'format-table-name tables))))

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
               || ', (select greatest(max(' || quote_ident(a.attname) || '), 1) from only '
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
          (parse-integer (cl-postgres:postgresql-notification-payload c)))))))

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

;;;
;;; Tools to query the PostgreSQL Schema, either source or target
;;;

(in-package :pgloader.pgsql)

(defun fetch-pgsql-catalog (target &key table including excluding)
  "Fetch PostgreSQL catalogs for the target database."
  (let ((catalog (make-catalog :name (db-name target))))
    (with-pgsql-connection (target)

      (when (and table (not including))
        ;; rewrite the table constraint as an including expression
        (let ((schema
               (or (table-schema table)
                   (make-schema :name (query-table-schema (table-name table))))))
         (setf including
               (list (cons (schema-name schema)
                           (list (table-name table)))))))

      (list-all-columns catalog
                        :table-type :table
                        :including including
                        :excluding excluding)

      (list-all-indexes catalog
                        :including including
                        :excluding excluding)

      (list-all-fkeys catalog
                      :including including
                      :excluding excluding))

    catalog))

(defun query-table-schema (table-name)
  "Get PostgreSQL schema name where to locate TABLE-NAME by following the
  current search_path rules. A PostgreSQL connection must be opened."
  (pomo:query (format nil "
  select nspname
    from pg_namespace n
    join pg_class c on n.oid = c.relnamespace
   where c.oid = '~a'::regclass;"
                      table-name) :single))


(defvar *table-type* '((:table    . "r")
		       (:view     . "v")
                       (:index    . "i")
                       (:sequence . "S"))
  "Associate internal table type symbol with what's found in PostgreSQL
  pg_class.relkind column.")

(defun filter-list-to-where-clause (filter-list
                                    &optional
                                      not
                                      (schema-col "table_schema")
                                      (table-col  "table_name"))
  "Given an INCLUDING or EXCLUDING clause, turn it into a PostgreSQL WHERE
   clause."
  (loop :for (schema . table-name-list) :in filter-list
     :append (mapcar (lambda (table-name)
                       (format nil "(~a = '~a' and ~a ~:[~;NOT ~]~~ '~a')"
                               schema-col schema table-col not table-name))
                     table-name-list)))

(defun list-all-columns (catalog
                         &key
                           (table-type :table)
                           including
                           excluding
                         &aux
                           (table-type-name (cdr (assoc table-type *table-type*))))
  "Get the list of PostgreSQL column names per table."
  (loop :for (schema-name table-name table-oid name type typmod notnull default)
     :in
     (pomo:query (format nil "
    select nspname, relname, c.oid, attname,
           t.oid::regtype as type,
           case when atttypmod > 0 then atttypmod - 4 else null end as typmod,
           attnotnull,
           case when atthasdef then def.adsrc end as default
      from pg_class c
           join pg_namespace n on n.oid = c.relnamespace
           left join pg_attribute a on c.oid = a.attrelid
           join pg_type t on t.oid = a.atttypid and attnum > 0
           left join pg_attrdef def on a.attrelid = def.adrelid
                                   and a.attnum = def.adnum

     where nspname !~~ '^pg_' and n.nspname <> 'information_schema'
           and relkind = '~a'
           ~:[~*~;and (~{~a~^~&~10t or ~})~]
           ~:[~*~;and (~{~a~^~&~10t and ~})~]

  order by nspname, relname, attnum"
                         table-type-name
                         including      ; do we print the clause?
                         (filter-list-to-where-clause including
                                                      nil
                                                      "n.nspname"
                                                      "c.relname")
                         excluding      ; do we print the clause?
                         (filter-list-to-where-clause excluding
                                                      nil
                                                      "n.nspname"
                                                      "c.relname")))
     :do
     (let* ((schema    (maybe-add-schema catalog schema-name))
            (table     (maybe-add-table schema table-name :oid table-oid))
            (field     (make-column :name name
                                    :type-name type
                                    :type-mod typmod
                                    :nullable (not notnull)
                                    :default default)))
       (add-field table field))
     :finally (return catalog)))

(defun list-all-indexes (catalog &key including excluding)
  "Get the list of PostgreSQL index definitions per table."
  (loop
     :for (schema-name name table-schema table-name primary unique sql conname condef)
     :in (pomo:query (format nil "
  select n.nspname,
         i.relname,
         rn.nspname,
         r.relname,
         indisprimary,
         indisunique,
         pg_get_indexdef(indexrelid),
         c.conname,
         pg_get_constraintdef(c.oid)
    from pg_index x
         join pg_class i ON i.oid = x.indexrelid
         join pg_class r ON r.oid = x.indrelid
         join pg_namespace n ON n.oid = i.relnamespace
         join pg_namespace rn ON rn.oid = r.relnamespace
         left join pg_constraint c ON c.conindid = i.oid
   where n.nspname !~~ '^pg_' and n.nspname <> 'information_schema'
         ~:[~*~;and (~{~a~^~&~10t or ~})~]
         ~:[~*~;and (~{~a~^~&~10t and ~})~]
order by n.nspname, r.relname"
                             including  ; do we print the clause?
                             (filter-list-to-where-clause including
                                                          nil
                                                          "n.nspname"
                                                          "i.relname")
                             excluding  ; do we print the clause?
                             (filter-list-to-where-clause excluding
                                                          nil
                                                          "n.nspname"
                                                          "i.relname")))
     :do (let* ((schema   (find-schema catalog schema-name))
                (tschema  (find-schema catalog table-schema))
                (table    (find-table tschema table-name))
                (pg-index
                 (make-index :name name
                             :schema schema
                             :table table
                             :primary primary
                             :unique unique
                             :columns nil
                             :sql sql
                             :conname (unless (eq :null conname) conname)
                             :condef  (unless (eq :null condef)  condef))))
           (maybe-add-index table name pg-index :key #'index-name))
     :finally (return catalog)))

(defun list-all-fkeys (catalog &key including excluding)
  "Get the list of PostgreSQL index definitions per table."
  (loop
     :for (schema-name table-name fschema-name ftable-name conname cols fcols
                       updrule delrule mrule deferrable deferred condef)
     :in
     (pomo:query (format nil "
 select n.nspname, c.relname, nf.nspname, cf.relname as frelname,
        conname,
        (select string_agg(attname, ',')
           from pg_attribute
          where attrelid = r.conrelid and array[attnum] <@ conkey
        ) as conkey,
        (select string_agg(attname, ',')
           from pg_attribute
          where attrelid = r.confrelid and array[attnum] <@ confkey
        ) as confkey,
        confupdtype, confdeltype, confmatchtype,
        condeferrable, condeferred,
        pg_catalog.pg_get_constraintdef(r.oid, true) as condef
   from pg_catalog.pg_constraint r
        JOIN pg_class c on r.conrelid = c.oid
        JOIN pg_namespace n on c.relnamespace = n.oid
        JOIN pg_class cf on r.confrelid = c.oid
        JOIN pg_namespace nf on cf.relnamespace = nf.oid
   where r.contype = 'f'
         AND c.relkind = 'r' and cf.relkind = 'r'
         AND n.nspname !~~ '^pg_' and n.nspname <> 'information_schema'
         AND nf.nspname !~~ '^pg_' and nf.nspname <> 'information_schema'
         ~:[~*~;and (~{~a~^~&~10t or ~})~]
         ~:[~*~;and (~{~a~^~&~10t and ~})~]
         ~:[~*~;and (~{~a~^~&~10t or ~})~]
         ~:[~*~;and (~{~a~^~&~10t and ~})~]"
                         including      ; do we print the clause (table)?
                         (filter-list-to-where-clause including
                                                      nil
                                                      "n.nspname"
                                                      "c.relname")
                         excluding      ; do we print the clause (table)?
                         (filter-list-to-where-clause excluding
                                                      nil
                                                      "n.nspname"
                                                      "c.relname")
                         including      ; do we print the clause (ftable)?
                         (filter-list-to-where-clause including
                                                      nil
                                                      "nf.nspname"
                                                      "cf.relname")
                         excluding      ; do we print the clause (ftable)?
                         (filter-list-to-where-clause excluding
                                                      nil
                                                      "nf.nspname"
                                                      "cf.relname")))
     :do (flet ((pg-fk-rule-to-action (rule)
                  (case rule
                    (#\a "NO ACTION")
                    (#\r "RESTRICT")
                    (#\c "CASCADE")
                    (#\n "SET NULL")
                    (#\d "SET DEFAULT")))
                (pg-fk-match-rule-to-match-clause (rule)
                  (case rule
                    (#\f "FULL")
                    (#\p "PARTIAL")
                    (#\s "SIMPLE"))))
           (let* ((schema   (find-schema catalog schema-name))
                  (table    (find-table schema table-name))
                  (fschema  (find-schema catalog fschema-name))
                  (ftable   (find-table fschema ftable-name))
                  (fk
                   (make-fkey :name (apply-identifier-case conname)
                              :condef condef
                              :table table
                              :columns (split-sequence:split-sequence #\, cols)
                              :foreign-table ftable
                              :foreign-columns (split-sequence:split-sequence #\, fcols)
                              :update-rule (pg-fk-rule-to-action updrule)
                              :delete-rule (pg-fk-rule-to-action delrule)
                              :match-rule (pg-fk-match-rule-to-match-clause mrule)
                              :deferrable deferrable
                              :initially-deferred deferred)))
             (if (and table ftable)
                 (add-fkey table fk)
                 (log-message :notice "Foreign Key ~a is ignored, one of its table is missing from pgloader table selection"
                              conname))))
     :finally (return catalog)))



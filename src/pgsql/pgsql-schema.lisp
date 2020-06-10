;;;
;;; Tools to query the PostgreSQL Schema, either source or target
;;;

(in-package :pgloader.pgsql)

(defun fetch-pgsql-catalog (dbname
                            &key
                              table
                              source-catalog
                              including
                              excluding
                              (variant :pgdg)
                              pgversion)
  "Fetch PostgreSQL catalogs for the target database. A PostgreSQL
   connection must be opened."
  (let* ((*identifier-case* :quote)
         (catalog   (make-catalog :name dbname))
         (including (cond ((and table (not including))
                           (make-including-expr-from-table table))

                          ((and source-catalog (not including))
                           (make-including-expr-from-catalog source-catalog))

                          (t
                           including))))
    (when (eq :pgdg variant)
      (list-all-sqltypes catalog
                         :including including
                         :excluding excluding))

    (list-all-columns catalog
                      :table-type :table
                      :including including
                      :excluding excluding)

    (list-all-indexes catalog
                      :including including
                      :excluding excluding
                      :pgversion pgversion)

    (when (eq :pgdg variant)
      (list-all-fkeys catalog
                      :including including
                      :excluding excluding)

      ;; fetch fkey we depend on with UNIQUE indexes but that have been
      ;; excluded from the target list, we still need to take care of them to
      ;; be able to DROP then CREATE those indexes again
      (list-missing-fk-deps catalog))

    (log-message :debug "fetch-pgsql-catalog: ~d tables, ~d indexes, ~d+~d fkeys"
                 (count-tables catalog)
                 (count-indexes catalog)
                 (count-fkeys catalog)
                 (loop :for table :in (table-list catalog)
                    :sum (loop :for index :in (table-index-list table)
                            :sum (length (index-fk-deps index)))))

    (when (and table (/= 1 (count-tables catalog)))
      (error "pgloader found ~d target tables for name ~a:~{~%  ~a~}"
             (count-tables catalog)
             (format-table-name table)
             (mapcar #'format-table-name (table-list catalog))))

    catalog))

(defun make-including-expr-from-catalog (catalog)
  "Return an expression suitable to be used as an :including parameter."
  (let (including current-schema)
    ;; The schema where to install the table or view in the target database
    ;; might be different from the schema where we find it in the source
    ;; table, thanks to the ALTER TABLE ... SET SCHEMA ... feature of
    ;; pgloader.
    ;;
    ;; The schema we want to lookup here is the target schema, so it's
    ;; (table-schema table) and not the schema where we found the table in
    ;; the catalog nested structure.
    ;;
    ;; Also, MySQL schema map to PostgreSQL databases, so we might have NIL
    ;; as a schema name here. In that case, we find the current PostgreSQL
    ;; schema and use that.
    (loop :for table :in (append (table-list catalog)
                                 (view-list catalog))
       :do (let* ((schema-name
                   (or (ensure-unquoted (schema-name (table-schema table)))
                       current-schema
                       (setf current-schema
                             (pomo:query "select current_schema()" :single))))
                  (table-expr
                   (format-table-name-as-including-exp table))
                  (schema-entry
                   (or (assoc schema-name including :test #'string=)
                       (progn (push (cons schema-name nil) including)
                              (assoc schema-name including :test #'string=)))))
             (push-to-end table-expr (cdr schema-entry))))
    ;; return the including alist
    including))

(defun make-including-expr-from-table (table)
  "Return an expression suitable to be used as an :including parameter."
  (let ((schema (or (table-schema table)
                    (query-table-schema table))))
    (list (cons (ensure-unquoted (schema-name schema))
                (list
                 (format-table-name-as-including-exp table))))))

(defun format-table-name-as-including-exp (table)
  "Return a table name suitable for a catalog lookup using ~ operator."
  (let ((table-name (table-name table)))
    (make-string-match-rule :target (ensure-unquoted table-name))))

(defun query-table-schema (table)
  "Get PostgreSQL schema name where to locate TABLE-NAME by following the
  current search_path rules. A PostgreSQL connection must be opened."
  (make-schema :name
               (pomo:query (sql "/pgsql/query-table-schema.sql"
                                (table-name table))
                           :single)))

(defun make-including-expr-from-view-names (view-names)
  "Turn MATERIALIZING VIEWs list of view names into an INCLUDING parameter."
  (let (including current-schema)
    (loop :for (schema-name . view-name) :in view-names
       :do (let* ((schema-name
                   (if schema-name
                       (ensure-unquoted schema-name)
                       (or
                        current-schema
                        (setf current-schema
                              (pomo:query "select current_schema()" :single)))))
                  (table-expr
                   (make-string-match-rule :target (ensure-unquoted view-name)))
                  (schema-entry
                   (or (assoc schema-name including :test #'string=)
                       (progn (push (cons schema-name nil) including)
                              (assoc schema-name including :test #'string=)))))
             (push-to-end table-expr (cdr schema-entry))))
    ;; return the including alist
    including))


(defvar *table-type*
  '((:table    . ("r" "f" "p"))   ; ordinary, foreign and partitioned
    (:view     . ("v"))
    (:index    . ("i"))
    (:sequence . ("S")))
  "Associate internal table type symbol with what's found in PostgreSQL
  pg_class.relkind column.")

(defun filter-list-to-where-clause (schema-filter-list
                                    &optional
                                      not
                                      (schema-col "table_schema")
                                      (table-col  "table_name"))
  "Given an INCLUDING or EXCLUDING clause, turn it into a PostgreSQL WHERE
   clause."
  (loop :for (schema . filter-list) :in schema-filter-list
     :append (mapcar (lambda (filter)
                       (typecase filter
                         (string-match-rule
                          (format nil "(~a = '~a' and ~a ~:[~;!~]= '~a')"
                                  schema-col
                                  schema
                                  table-col
                                  not
                                  (string-match-rule-target filter)))
                         (regex-match-rule
                          (format nil "(~a = '~a' and ~a ~:[~;NOT ~]~~ '~a')"
                                  schema-col
                                  schema
                                  table-col
                                  not
                                  (regex-match-rule-target filter)))))
                     filter-list)))

(defun normalize-extra (extra)
  (cond ((string= "auto_increment" extra) :auto-increment)))

(defun list-all-columns (catalog
                         &key
                           (table-type :table)
                           including
                           excluding
                         &aux
                           (table-type-name (cdr (assoc table-type *table-type*))))
  "Get the list of PostgreSQL column names per table."
  (loop :for (schema-name table-name table-oid
                          name type typmod notnull default extra)
     :in (query nil (sql "/pgsql/list-all-columns.sql"
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
            (field     (make-column :table table
                                    :name name
                                    :type-name type
                                    :type-mod typmod
                                    :nullable (not notnull)
                                    :default default
                                    :transform-default nil
                                    :extra (normalize-extra extra))))
       (add-field table field))
     :finally (return catalog)))

(defun list-all-indexes (catalog &key including excluding pgversion)
  "Get the list of PostgreSQL index definitions per table."
  (loop
     :for (schema-name name oid
                       table-schema table-name
                       primary unique cols sql conname condef)
     :in (query nil (sql (sql-url-for-variant "pgsql"
                                              "list-all-indexes.sql"
                                              pgversion)
                         including      ; do we print the clause?
                         (filter-list-to-where-clause including
                                                      nil
                                                      "rn.nspname"
                                                      "r.relname")
                         excluding      ; do we print the clause?
                         (filter-list-to-where-clause excluding
                                                      nil
                                                      "rn.nspname"
                                                      "r.relname")))
     :do (let* ((schema   (find-schema catalog schema-name))
                (tschema  (find-schema catalog table-schema))
                (table    (find-table tschema table-name))
                (columns  (parse-index-column-names cols sql))
                (pg-index
                 (make-index :name (ensure-quoted name)
                             :oid oid
                             :schema schema
                             :table table
                             :primary primary
                             :unique unique
                             :columns columns
                             :sql sql
                             :conname (unless (eq :null conname)
                                        (ensure-quoted conname))
                             :condef  (unless (eq :null condef)
                                        condef))))
           (maybe-add-index table name pg-index :key #'index-name))
     :finally (return catalog)))

(defun list-all-fkeys (catalog &key including excluding)
  "Get the list of PostgreSQL index definitions per table."
  (loop
     :for (schema-name table-name fschema-name ftable-name
                       conoid pkeyoid conname condef
                       cols fcols
                       updrule delrule mrule deferrable deferred)
     :in (query nil (sql "/pgsql/list-all-fkeys.sql"
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
                  (pkey     (find pkeyoid (table-index-list ftable)
                                  :test #'=
                                  :key #'index-oid))
                  (fk
                   (make-fkey :name (ensure-quoted conname)
                              :oid conoid
                              :pkey pkey
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
             ;; add the fkey reference to the pkey index too
             (unless (find conoid
                           (index-fk-deps pkey)
                           :test #'=
                           :key #'fkey-oid)
               (push-to-end fk (index-fk-deps pkey)))
             ;; check that both tables are in pgloader's scope
             (if (and table ftable)
                 (add-fkey table fk)
                 (log-message :notice "Foreign Key ~a is ignored, one of its table is missing from pgloader table selection"
                              conname))))
     :finally (return catalog)))

(defun list-missing-fk-deps (catalog)
  "Add in the CATALOG the foreign keys we don't have to deal with directly
   but that the primary keys we are going to DROP then CREATE again depend
   on: we need to take care of those first."
  (destructuring-bind (pkey-oid-hash-table pkey-oid-list fkey-oid-list)
      (loop :with pk-hash := (make-hash-table)
         :for table :in (table-list catalog)
         :append (mapcar #'index-oid (table-index-list table)) :into pk
         :append (mapcar #'fkey-oid (table-fkey-list table)) :into fk
         :do (loop :for index :in (table-index-list table)
                :do (setf (gethash (index-oid index) pk-hash) index))
         :finally (return (list pk-hash pk fk)))

    (when pkey-oid-list
      (loop :for (schema-name table-name fschema-name ftable-name
                              conoid conname condef index-oid)
         :in (query nil (sql "/pgsql/list-missing-fk-deps.sql"
                             pkey-oid-list
                             (or fkey-oid-list (list -1))))
         ;;
         ;; We don't need to reference the main catalog entries for the tables
         ;; here, as the only goal is to be sure to DROP then CREATE again the
         ;; existing constraint that depend on the UNIQUE indexes we have to
         ;; DROP then CREATE again.
         ;;
         :do (let* ((schema  (make-schema :name schema-name))
                    (table   (make-table :name table-name :schema schema))
                    (fschema (make-schema :name fschema-name))
                    (ftable  (make-table :name ftable-name :schema fschema))
                    (index   (gethash index-oid pkey-oid-hash-table)))
               (push-to-end (make-fkey :name conname
                                       :oid conoid
                                       :condef condef
                                       :table table
                                       :foreign-table ftable)
                            (index-fk-deps index)))))))


;;;
;;; Extra utilities to introspect a PostgreSQL schema.
;;;
(defun list-schemas ()
  "Return the list of PostgreSQL schemas in the already established
   PostgreSQL connection."
  (pomo:query "SELECT nspname FROM pg_catalog.pg_namespace;" :column))

(defun list-search-path ()
  "Return the current list of schemas in the Search Path"
  (pomo:query
   "SELECT name FROM unnest(pg_catalog.current_schemas(false)) as t(name);"
   :column))

(defun get-current-database ()
  "Get the current database name. The catalog name and the connection string
   name may be different, so just ask PostgreSQL here."
  (pomo:query "select current_database();" :single))

(defun list-table-oids (catalog &key (variant :pgdg))
  "Return an hash table mapping TABLE-NAME to its OID for all table in the
   TABLE-NAMES list. A PostgreSQL connection must be established already."
  (let* ((table-list  (table-list catalog))
         (oidmap      (make-hash-table :size (length table-list) :test #'equal)))
    (when table-list
      (loop :for (name oid)
         :in (ecase variant
               (:pgdg
                ;; use the SELECT ... FROM (VALUES ...) variant
                (query nil (sql "/pgsql/list-table-oids.sql"
                                (mapcar #'format-table-name table-list))))
               (:redshift
                ;; use the TEMP TABLE variant in Redshift, which doesn't
                ;; have proper support for VALUES (landed in PostgreSQL 8.2)
                (query nil
                       "create temp table pgloader_toids(tnsp text, tnam text)")
                (query nil
                       (format
                        nil
                        "insert into pgloader_toids values ~{(~a)~^,~};"
                        (mapcar (lambda (table)
                                  (format nil "'~a', '~a'"
                                          (schema-name (table-schema table))
                                          (table-name table)))
                                table-list)))
                (query nil
                       (sql "/pgsql/list-table-oids-from-temp-table.sql"))))
         :do (setf (gethash name oidmap) oid)))
    oidmap))



;;;
;;; PostgreSQL specific support for extensions and user defined data types.
;;;
(defun list-all-sqltypes (catalog &key including excluding)
  "Set the catalog's schema extension list and sqltype list"
  (loop :for (schema-name extension-name type-name enum-values)
     :in (query nil (sql "/pgsql/list-all-sqltypes.sql"
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
            (sqltype
             (make-sqltype :name (ensure-quoted type-name)
                           :schema schema
                           :type (when enum-values :enum)
                           :extra (when (and enum-values
                                             (not (eq enum-values :null)))
                                    (coerce enum-values 'list)))))

       (if (and extension-name (not (eq :null extension-name)))
           ;; then create extension will create the type
           (maybe-add-extension schema extension-name)

           ;; only create a specific entry for types that we need to create
           ;; ourselves, when extension is not null "create extension" is
           ;; going to take care of creating the type.
           (add-sqltype schema sqltype)))
     :finally (return catalog)))



;;;
;;; Extra utils like parsing a list of column names from an index definition.
;;;
(defun parse-index-column-names (columns index-definition)
  "Return a list of column names for the given index."
  (if (and columns (not (eq :null columns)))
      ;; the normal case, no much parsing to do, the data has been prepared
      ;; for us in the SQL query
      (split-sequence:split-sequence #\, columns)

      ;; the redshift variant case, where there's no way to string_agg or
      ;; even array_to_string(array_agg(...)) and so we need to parse the
      ;; index-definition instead.
      ;;
      ;; CREATE UNIQUE INDEX pg_amproc_opc_proc_index ON pg_amproc USING btree (amopclaid, amprocsubtype, amprocnum)
      (when index-definition
        (let ((open-paren-pos  (position #\( index-definition))
              (close-paren-pos (position #\) index-definition)))
          (when (and open-paren-pos close-paren-pos)
            (mapcar (lambda (colname) (string-trim " " colname))
                    (split-sequence:split-sequence #\,
                                                   index-definition
                                                   :start (+ 1 open-paren-pos)
                                                   :end close-paren-pos)))))))

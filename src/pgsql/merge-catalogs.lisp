;;;
;;; To support pre-existing database targets we need to be able to merge the
;;; catalog we get from the source database and the catalog we fetch in the
;;; already prepared target database: that's the typical scenario when using
;;; an ORM to define the schema.
;;;
;;; Using an ORM to define a database schema is considered very bad practice
;;; for very good reasons. pgloader goal is to facilitate migrations to
;;; PostgreSQL, the education is generally welcome in later steps.
;;;

(in-package :pgloader.pgsql)

(defun merge-catalogs (source-catalog target-catalog)
  "In order for the data loading to be as fast as possible, we DROP the
   constraints and indexes on the target table. Once the data is loaded we
   want to install the same constraints as found pre-existing in the
   TARGET-CATALOG rather than the one we casted from the SOURCE-CATALOG.

   Also, we want to recheck the cast situation and the selected
   transformation functions of each column."

  (let (skip-list)
    (loop :for source-schema :in (catalog-schema-list source-catalog)
       :do (let* ((schema-name
                   ;; MySQL schema map to PostgreSQL databases, so we might
                   ;; have NIL as a schema name here. Find the current
                   ;; PostgreSQL schema instead of NIL.
                   (or (ensure-unquoted (schema-name source-schema))
                       (pomo:query "select current_schema()" :single)))
                  (target-schema
                   (find-schema target-catalog schema-name)))

             (unless target-schema
               ;; it could be that we found nothing in the schema because of
               ;; including/excluding rules, or that the schema doesn't
               ;; exists, let's double check
               (let ((schema-list (list-schemas)))
                 (if (member schema-name schema-list :test #'string=)
                     (error "pgloader failed to find anything in schema ~s in target catalog."
                            schema-name)
                     (error "pgloader failed to find schema ~s in target catalog."
                            schema-name))))

             (loop :for source-table :in (schema-table-list source-schema)
                :for target-table := (find-table target-schema
                                                 (ensure-unquoted
                                                  (table-name source-table)))
                :do (if target-table
                        (progn
                          ;; re-use indexes and fkeys from target-catalog
                          (setf (table-oid source-table)
                                (table-oid target-table)

                                (table-index-list source-table)
                                (table-index-list target-table)

                                (table-fkey-list source-table)
                                (table-fkey-list target-table)

                                ;; special case for triggers: we don't DROP them,
                                ;; we only ALTER TABLE ... DISABLE TRIGGERS then
                                ;; ENABLE them again; so we only have to refrain
                                ;; from installing the source-catalog ones here.
                                (table-trigger-list source-table)
                                nil)

                          (check-table-columns schema-name
                                               source-table
                                               target-table))

                        ;;
                        ;; We failed to find a matching table for the source
                        ;; table, let the user know then remove the table from
                        ;; the source catalogs.
                        ;;
                        (progn
                          (log-message :error
                                       "pgloader failed to find target table for source ~s.~s with name ~s in target catalog"
                                       (schema-source-name source-schema)
                                       (table-source-name source-table)
                                       (table-name source-table))
                          (push-to-end source-table skip-list))))))

    (when skip-list
      (loop :for table :in skip-list
         :do (let ((schema (table-schema table)))
               (log-message :log "Skipping ~a" (format-table-name table))
               (setf (schema-table-list schema)
                     (delete table (schema-table-list schema))))))))

(defun check-table-columns (schema-name source-table target-table)
  (loop :for source-column :in (table-column-list source-table)
     :do (let* ((col-name      (ensure-unquoted
                                (column-name source-column)))
                ;;
                ;; Apply PostgreSQL case rules for identifiers: compare
                ;; UPCASE versions of them...
                ;;
                (target-column (find (string-upcase col-name)
                                     (table-field-list target-table)
                                     :key (lambda (column)
                                            (string-upcase
                                             (column-name column)))
                                     :test #'string=)))
           (unless target-column
             (error "pgloader failed to find column ~s.~s.~s in target table ~s"
                    schema-name
                    (table-source-name source-table)
                    (column-name source-column)
                    (format-table-name target-table)))

           (unless (same-type-p source-column target-column)
             (log-message :warning "Source column ~s.~s.~s is casted to type ~s which is not the same as ~s, the type of current target database column ~s.~s.~s."
                          schema-name
                          (table-source-name source-table)
                          (column-name source-column)
                          (column-type-name source-column)
                          (column-type-name target-column)
                          (ensure-unquoted
                           (schema-name (table-schema target-table)))
                          (ensure-unquoted
                           (table-name target-table))
                          (ensure-unquoted
                           (column-name target-column)))))))

(defvar *type-name-mapping*
  '(("int"         "integer")
    ("serial"      "integer")
    ("bigserial"   "bigint")
    ("float"       "double precision")
    ("char"        "character")
    ("varchar"     "character varying")
    ("timestamp"   "timestamp without time zone")
    ("timestamptz" "timestamp with time zone")
    ("decimal"     "numeric"))
  "Alternative spellings for some type names.")

(defun get-type-name (column)
  "Support SQLTYPE indirection if needed."
  (let ((typname (column-type-name column)))
    (etypecase typname
      (string  typname)
      (sqltype (get-column-type-name-from-sqltype column)))))

(defun same-type-p (source-column target-column)
  "Evaluate if SOURCE-COLUMN and TARGET-COLUMN selected type names are
   similar enough that we may continue with the migration."
  (let ((source-type-name (get-type-name source-column))
        (target-type-name (column-type-name target-column)))
    (or (string-equal source-type-name target-type-name)
        (member target-type-name (cdr (assoc source-type-name *type-name-mapping*
                                             :test #'string=))
                :test #'string-equal))))

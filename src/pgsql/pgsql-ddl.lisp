;;;
;;; PostgreSQL fkey support implementation as a Target Database
;;;

(in-package :pgloader.pgsql)

;;;
;;; Schemas
;;;
(defmethod format-create-sql ((schema schema) &key (stream nil) if-not-exists)
  (format stream "CREATE SCHEMA~@[~IF NOT EXISTS~] ~a;"
          if-not-exists
          (schema-name schema)))

(defmethod format-drop-sql ((schema schema) &key (stream nil) cascade if-exists)
  (format stream "DROP SCHEMA~@[ IF EXISTS~] ~a~@[ CASCADE~];"
          if-exists (schema-name schema) cascade))


;;;
;;; Types
;;;
(defmethod format-create-sql ((sqltype sqltype) &key (stream nil) if-not-exists)
  (declare (ignore if-not-exists))
  (ecase (sqltype-type sqltype)
    ((:enum :set)
     (format stream "CREATE TYPE ~a AS ENUM (~{'~a'~^, ~});"
             (sqltype-name sqltype)
             (sqltype-extra sqltype)))))

(defmethod format-drop-sql ((sqltype sqltype) &key (stream nil) cascade if-exists)
  (format stream "DROP TYPE~:[~; IF EXISTS~] ~a~@[ CASCADE~];"
          if-exists (sqltype-name sqltype) cascade))


;;;
;;; Tables
;;;
(defmethod format-create-sql ((table table) &key (stream nil) if-not-exists)
  ;;
  ;; In case stream would be nil, which means return a string, we use this
  ;; with-output-to-string form and format its output in stream...
  ;;
  (format stream "~a"
          (with-output-to-string (s)
            (format s "CREATE TABLE~:[~; IF NOT EXISTS~] ~a ~%(~%"
                    if-not-exists
                    (format-table-name table))
            (let ((max (reduce #'max
                               (mapcar #'length
                                       (mapcar #'column-name
                                               (table-column-list table))))))
              (loop
                 :for (col . last?) :on (table-column-list table)
                 :do (progn
                       (format s "  ")
                       (format-create-sql col
                                          :stream s
                                          :pretty-print t
                                          :max-column-name-length max)
                       (format s "~:[~;,~]~%" last?))))
            (format s ");~%"))))

(defmethod format-drop-sql ((table table) &key (stream nil) cascade (if-exists t))
  "Return the PostgreSQL DROP TABLE IF EXISTS statement for TABLE-NAME."
  (format stream
          "DROP TABLE~:[~; IF EXISTS~] ~a~@[ CASCADE~];"
          if-exists (format-table-name table) cascade))


;;;
;;; Columns
;;;
(defun get-column-type-name-from-sqltype (column)
  "Return the column type name. When column-type is a sqltype, the sqltype
   might be either an ENUM or a SET. In the case of a SET, we want an array
   type to be defined here."
  (let ((type-name (column-type-name column)))
    (typecase type-name
      (sqltype (ecase (sqltype-type type-name)
                 (:enum (sqltype-name type-name))
                 (:set  (format nil "~a[]" (sqltype-name type-name)))))
      (string  type-name))))

(defmethod format-create-sql ((column column)
                              &key
                                (stream nil)
                                if-not-exists
                                pretty-print
                                ((:max-column-name-length max)))
  (declare (ignore if-not-exists))
  (format stream
          "~a~vt~a~:[~*~;~a~]~:[ not null~;~]~:[~; default ~a~]"
          (column-name column)
          (if pretty-print (if max (+ 3 max) 22) 1)
          (get-column-type-name-from-sqltype column)
          (column-type-mod column)
          (column-type-mod column)
          (column-nullable column)
          (column-default column)
          (format-default-value column)))

(defvar *pgsql-default-values*
  '((:null              . "NULL")
    (:current-date      . "CURRENT_DATE")
    (:current-timestamp . "CURRENT_TIMESTAMP")
    (:generate-uuid     . "uuid_generate_v1()"))
  "Common normalized default values and their PostgreSQL spelling.")

(defmethod format-default-value ((column column) &key (stream nil))
  (let* ((default       (column-default column))
         (clean-default (cdr (assoc default *pgsql-default-values*)))
         (transform     (column-transform column)))
    (or clean-default
        (if transform
            (let* ((transformed-default
                    (handler-case
                        (funcall transform default)
                      (condition (c)
                        (log-message :warning
                                     "Failed to transform default value ~s: ~a"
                                     default c)
                        ;; can't transform: return nil
                        nil)))
                   (transformed-column
                    (make-column :default transformed-default)))
              (format-default-value transformed-column))
            (if default
                (format stream "'~a'" default)
                (format stream "NULL"))))))


;;;
;;; Indexes
;;;
(defmethod format-create-sql ((index index) &key (stream nil) if-not-exists)
  (declare (ignore if-not-exists))
  (let* ((table      (index-table index))
         (index-name (if (and *preserve-index-names*
                              (not (string-equal "primary" (index-name index)))
                              (table-oid (index-table index)))
                         (index-name index)

                         ;; in the general case, we build our own index name.
                         (format nil "idx_~a_~a"
                                 (table-oid (index-table index))
                                 (index-name index))))
	 (index-name (apply-identifier-case index-name)))
    (cond
      ((or (index-primary index)
           (and (index-condef index) (index-unique index)))
       (values
        ;; ensure good concurrency here, don't take the ACCESS EXCLUSIVE
        ;; LOCK on the table before we have the index done already
        (or (index-sql index)
            (format stream
                    "CREATE UNIQUE INDEX ~a ON ~a (~{~a~^, ~})~@[ WHERE ~a~];"
                    index-name
                    (format-table-name table)
                    (index-columns index)
                    (index-filter index)))
        (format nil
                ;; don't use the index schema name here, PostgreSQL doesn't
                ;; like it, might be implicit from the table's schema
                ;; itself...
                "ALTER TABLE ~a ADD ~a USING INDEX ~a;"
                (format-table-name table)
                (cond ((index-primary index) "PRIMARY KEY")
                      ((index-unique index) "UNIQUE"))
                index-name)))

      ((index-condef index)
       (format stream "ALTER TABLE ~a ADD ~a;"
               (format-table-name table)
               (index-condef index)))

      (t
       (or (index-sql index)
           (format stream
                   "CREATE~:[~; UNIQUE~] INDEX ~a ON ~a (~{~a~^, ~})~@[ WHERE ~a~];"
                   (index-unique index)
                   index-name
                   (format-table-name table)
                   (index-columns index)
                   (index-filter index)))))))

(defmethod format-drop-sql ((index index) &key (stream nil) cascade if-exists)
  (let* ((schema-name (schema-name (index-schema index)))
         (index-name  (index-name index)))
    (cond ((index-conname index)
           ;; here always quote the constraint name, currently the name
           ;; comes from one source only, the PostgreSQL database catalogs,
           ;; so don't question it, quote it.
           (format stream
                   "ALTER TABLE ~a DROP CONSTRAINT~:[~; IF EXISTS~] ~a~@[ CASCADE~];"
                   (format-table-name (index-table index))
                   if-exists
                   (index-conname index)
                   cascade))

          (t
           (format stream "DROP INDEX~:[~; IF EXISTS~] ~@[~a.~]~a~@[ CASCADE~];"
                   if-exists schema-name index-name cascade)))))


;;;
;;; Foreign Keys
;;;
(defmethod format-create-sql ((fk fkey) &key (stream nil) if-not-exists)
  (declare (ignore if-not-exists))
  (if (and (fkey-name fk) (fkey-condef fk))
      (format stream "ALTER TABLE ~a ADD CONSTRAINT ~a ~a"
              (format-table-name (fkey-table fk))
              (fkey-name fk)
              (fkey-condef fk))
      (format stream
              "ALTER TABLE ~a ADD ~@[CONSTRAINT ~a ~]FOREIGN KEY(~{~a~^,~}) REFERENCES ~a(~{~a~^,~})~:[~*~; ON UPDATE ~a~]~:[~*~; ON DELETE ~a~]"
              (format-table-name (fkey-table fk))
              (fkey-name fk)            ; constraint name
              (fkey-columns fk)
              (format-table-name (fkey-foreign-table fk))
              (fkey-foreign-columns fk)
              (fkey-update-rule fk)
              (fkey-update-rule fk)
              (fkey-delete-rule fk)
              (fkey-delete-rule fk))))

(defmethod format-drop-sql ((fk fkey) &key (stream nil) cascade if-exists)
  (let* ((constraint-name (fkey-name fk))
         (table-name      (format-table-name (fkey-table fk))))
    (format stream "ALTER TABLE ~a DROP CONSTRAINT~:[~; IF EXISTS~] ~a~@[ CASCADE~];"
            table-name if-exists constraint-name cascade)))


;;;
;;; Triggers
;;;
(defmethod format-create-sql ((trigger trigger) &key (stream nil) if-not-exists)
  (declare (ignore if-not-exists))
  (format stream
          "CREATE TRIGGER ~a ~a ON ~a FOR EACH ROW EXECUTE PROCEDURE ~a()"
          (trigger-name trigger)
          (trigger-action trigger)
          (format-table-name (trigger-table trigger))
          (trigger-procedure-name trigger)))

(defmethod format-drop-sql ((trigger trigger) &key (stream nil) cascade if-exists)
  (format stream
          "DROP TRIGGER~:[~; IF EXISTS~] ~a ON ~a~@[ CASCADE~];"
          if-exists
          (trigger-name trigger)
          (format-table-name (trigger-table trigger))
          cascade))


;;;
;;; Procedures
;;;
(defmethod format-create-sql ((procedure procedure) &key (stream nil) if-not-exists)
  (declare (ignore if-not-exists))
  (format stream
          "CREATE OR REPLACE FUNCTION ~a() RETURNS ~a LANGUAGE ~a AS $$~%~a~%$$;"
          (procedure-name procedure)
          (procedure-returns procedure)
          (procedure-language procedure)
          (procedure-body procedure)))

(defmethod format-drop-sql ((procedure procedure) &key (stream nil) cascade if-exists)
  (format stream
          "DROP FUNCTION~:[~; IF EXISTS~] ~a()~@[ CASCADE~];"
          if-exists (procedure-name procedure) cascade))


;;;
;;; Comments
;;;

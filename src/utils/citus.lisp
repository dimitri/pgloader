;;;
;;; Citus support in pgloader allows to declare what needs to change in the
;;; source schema in terms of Citus concepts: reference and distributed
;;; table.
;;;

#|
   distribute billers using id
   distribute bills using biller_id
   distribute receivable_accounts using biller_id
   distribute payments using biller_id

   distribute splits using biller_id
                      from receivable_accounts

   distribute ach_accounts as reference table
|#


(in-package #:pgloader.catalog)

(defstruct citus-reference-table table)
(defstruct citus-distributed-table table using from)

(defun citus-distribute-schema (catalog distribution-rules)
  "Distribute a CATALOG with given user provided DISTRIBUTION-RULES."
  (loop :for rule :in distribution-rules
     :do (let ((table (citus-find-table catalog (citus-rule-table rule))))
           (apply-citus-rule rule table))))

(defun citus-rule-table (rule)
  (etypecase rule
    (citus-reference-table (citus-reference-table-table rule))
    (citus-distributed-table (citus-distributed-table-table rule))))

(defun citus-find-table (catalog table)
  (let* ((table-name  (table-name table))
         (schema-name (schema-name (table-schema table))))
    (find-table (find-schema catalog schema-name) table-name)))

(defgeneric apply-citus-rule (rule table)
  (:documentation "Apply a Citus distribution RULE to given TABLE."))

(defmethod apply-citus-rule ((rule citus-reference-table) (table table))
  ;; for a reference table, we have nothing to do really.
  (setf (table-citus-rule table) rule))

(defmethod apply-citus-rule ((rule citus-distributed-table) (table table))
  (setf (table-citus-rule table) rule)

  ;;
  ;; Replace the TABLE placeholders in the :FROM slot of the rule with the
  ;; tables from the catalogs.
  ;;
  (when (citus-distributed-table-from rule)
    (let ((catalog (schema-catalog (table-schema table))))
     (map-into (citus-distributed-table-from rule)
               (lambda (from) (citus-find-table catalog from))
               (citus-distributed-table-from rule))))

  ;; ok now we need to check if the USING column exists or if we need to add
  ;; it to our model
  (let ((column (find (column-name (citus-distributed-table-using rule))
                      (table-field-list table)
                      :test #'string=
                      :key #'column-name)))
    (if column

        ;; add it to the PKEY definition, in first position
        (add-column-to-pkey table
                            (column-name (citus-distributed-table-using rule)))

        ;; The column doesn't exist, we need to find it in the :FROM rule's
        ;; list. The :FROM slot of the rule is a list of tables to
        ;; "traverse" when backfilling the data. The list follows the
        ;; foreign-key relationships from TABLE to the source of the
        ;; distribution key.
        ;;
        ;; To find the column definition to add to the current TABLE, look
        ;; it up in the last entry of the FROM rule's list.
        (let* ((last-from-rule (car (last (citus-distributed-table-from rule))))
               (column-definition
                (find (column-name (citus-distributed-table-using rule))
                      (table-field-list last-from-rule)
                      :test #'string=
                      :key #'column-name))
               (new-column
                (make-column :name (column-name column-definition)
                             :type-name (column-type-name column-definition)
                             :nullable (column-nullable column-definition)
                             :transform (column-transform column-definition))))
          ;;
          ;; Here also we need to add the new column to the PKEY definition,
          ;; in first position.
          ;;
          (add-column-to-pkey table (column-name new-column))

          ;;
          ;; We need to backfill the distribution key in the data, which
          ;; we're implementing with a JOIN when we SELECT from the source
          ;; table. We add the new field here.
          ;;
          (push new-column (table-field-list table))
          (push new-column (table-column-list table))))))


(defun add-column-to-pkey (table column-name)
  "Add COLUMN in the first position of the TABLE's primary key index."
  (let* ((index  (find-if #'index-primary (table-index-list table)))
         (idxcol (find column-name (index-columns index) :test #'string=)))
    (assert (not (null index)))
    (unless idxcol
      ;; add a new column
      (push column-name (index-columns index))
      ;; now remove origin schema sql and condef, we need to redo them
      (setf (index-sql index) nil)
      (setf (index-condef index) nil)

      ;; now tweak the fkey definitions that are using this index
      (loop :for fkey :in (index-fk-deps index)
         :do (push column-name (fkey-columns fkey))
         :do (push column-name (fkey-foreign-columns fkey))
         :do (setf (fkey-condef fkey) nil)))))


(defun format-citus-join-clause (table distribution-rule)
  "Format a JOIN clause to backfill the distribution key data in tables that
   are referencing (even indirectly) the main distribution table."
  (with-output-to-string (s)
    (loop :for current-table := table :then rel
       :for rel :in (citus-distributed-table-from distribution-rule)
       :do (let* ((fkey
                   (find (ensure-unquoted (table-name rel))
                         (table-fkey-list current-table)
                         :test #'string=
                         :key (lambda (fkey)
                                (ensure-unquoted
                                 (table-name (fkey-foreign-table fkey))))))
                  (ftable (fkey-foreign-table fkey)))
             (format s
                     " JOIN ~s.~s"
                     (schema-source-name (table-schema ftable))
                     (table-source-name ftable))
             ;;
             ;; Skip the first column in the fkey definition, that's the
             ;; distribution key that was just added by pgloader: we don't
             ;; have it on the source database, we are going to create it on
             ;; the target database.
             ;;
             (loop :for first := t :then nil
                :for c :in (cdr (fkey-columns fkey))
                :for fc :in (cdr (fkey-foreign-columns fkey))
                :do (format s
                            " ~:[AND~;ON~] ~a.~a = ~a.~a"
                            first
                            (table-source-name (fkey-table fkey))
                            c
                            (table-source-name (fkey-foreign-table fkey))
                            fc))))))

(defun citus-format-sql-select (source-table target-table)
  "Return the SQL statement to use to fetch data from the COPY context,
   including backfilling the distribution key in related tables."

  ;;
  ;; SELECT from.id, id, ... from source join from-table ...
  ;;
  ;; So we must be careful to prefix the column names with the
  ;; proper table name, because of the join(s), and the first column
  ;; in the output is taken from the main FROM table (the last one
  ;; in the rule).
  ;;
  (let* ((last-from-rule
          (car (last (citus-distributed-table-from
                      (table-citus-rule target-table)))))
         (cols
          (append (list
                   (format nil "~a.~a"
                           (table-name last-from-rule)
                           (column-name (first (table-field-list source-table)))))
                  (mapcar (lambda (field)
                            (format nil "~a.~a"
                                    (table-name source-table)
                                    (column-name field)))
                          (rest (table-field-list source-table)))))
         (joins
          (format-citus-join-clause source-table
                                    (table-citus-rule target-table))))
    (format nil
            "SELECT ~{~a::text~^, ~} FROM ~s.~s ~a"
            cols
            (schema-source-name (table-schema source-table))
            (table-source-name source-table)
            joins)))

(defun citus-backfill-table-p (table)
  "Returns non-nil when given TABLE should be backfilled with the
   distribution key."
  (and (table-citus-rule table)
       (typep (table-citus-rule table) 'citus-distributed-table)
       (not (null (citus-distributed-table-from (table-citus-rule table))))))

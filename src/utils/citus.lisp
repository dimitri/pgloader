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

  ;; ok now we need to check if the USING column exists or if we need to add
  ;; it to our model
  (let ((column (find (column-name (citus-distributed-table-using rule))
                      (table-field-list table)
                      :test #'string=
                      :key #'column-name)))
    (assert (not (null column)))

    (if column

        ;; add it to the PKEY definition, in first position
        (let* ((index  (find-if #'index-primary (table-index-list table)))
               (idxcol (find (column-name (citus-distributed-table-using rule))
                             (index-columns index)
                             :test #'string=)))
          (assert (not (null index)))
          (unless idxcol
            ;; add a new column
            (push (column-name (citus-distributed-table-using rule))
                  (index-columns index))
            ;; now remove origin schema sql and condef, we need to redo them
            (setf (index-sql index) nil)
            (setf (index-condef index) nil)))

        ;; the column doesn't exist, we need to find it in the :FROM rule
        (let* ((from-table
                (citus-find-table (schema-catalog (table-schema table))
                                  (citus-distributed-table-from rule)))
               (column-definition
                (find (column-name (citus-distributed-table-using rule))
                      (table-field-list from-table)
                      :test #'string=
                      :key #'column-name)))
          (assert (not (null from-table)))
          (push (make-column :name (column-name column-definition)
                             :type-name (column-type-name column-definition)
                             :nullable (column-nullable column-definition)
                             :transform (column-transform column-definition))
                (table-column-list table))))))

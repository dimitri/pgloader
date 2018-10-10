#|
   distribute billers using id
   distribute bills using biller_id
   distribute receivable_accounts using biller_id
   distribute payments using biller_id

   distribute splits using biller_id
                      from receivable_accounts

   distribute ach_accounts as reference table
|#

(in-package :pgloader.parser)

(defun create-table-from-dsn-table-name (dsn-table-name
                                         &optional (schema-name "public"))
  (let ((table (create-table (cdr (second dsn-table-name)))))
    (unless (table-schema table)
      (setf (table-schema table)
            (make-schema :catalog nil
                         :source-name schema-name
                         :name (apply-identifier-case schema-name))))
    table))

(defrule distribute-reference (and kw-distribute dsn-table-name
                                   kw-as kw-reference kw-table)
  (:lambda (d-r)
    (make-citus-reference-table :table (create-table-from-dsn-table-name d-r))))

(defrule distribute-using (and kw-distribute dsn-table-name
                               kw-using maybe-quoted-namestring)
  (:lambda (d-u)
    (make-citus-distributed-table :table (create-table-from-dsn-table-name d-u)
                                  :using (make-column :name (fourth d-u)))))

(defrule distribute-using-from (and kw-distribute dsn-table-name
                                    kw-using maybe-quoted-namestring
                                    kw-from (+ maybe-quoted-namestring))
  (:lambda (d-u-f)
    (make-citus-distributed-table :table (create-table-from-dsn-table-name d-u-f)
                                  :using (make-column :name (fourth d-u-f))
                                  :from (apply #'create-table (sixth d-u-f)))))

(defrule distribute-commands (+ (or distribute-using-from
                                    distribute-using
                                    distribute-reference))
  (:lambda (commands)
    (cons :distribute commands)))

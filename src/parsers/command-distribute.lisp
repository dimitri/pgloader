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
    (make-citus-reference-rule :table (create-table-from-dsn-table-name d-r))))

(defrule distribute-using (and kw-distribute dsn-table-name
                               kw-using maybe-quoted-namestring)
  (:lambda (d-u)
    (make-citus-distributed-rule :table (create-table-from-dsn-table-name d-u)
                                 :using (make-column :name (fourth d-u)))))

;;;
;;; The namestring rule allows for commas and we use them as a separator
;;; here, so we need to have our own table name parsing. That's a bummer,
;;; maybe we should revisit the whole table names parsing code?
;;;
(defrule distribute-from-tablename
    (or double-quoted-namestring
        quoted-namestring
        (and (or #\_ (alpha-char-p character))
             (* (or (alpha-char-p character)
                    (digit-char-p character)))))
  (:text t))

(defrule maybe-qualified-dist-from-table-name
    (and distribute-from-tablename (? (and "." distribute-from-tablename)))
  (:lambda (name)
    (if (second name)
        (cons (first name) (second (second name)))
        (cons "public" (first name)))))

(defrule distribute-from-list (+ (and maybe-qualified-dist-from-table-name
                                      (? (and "," ignore-whitespace))))
  (:lambda (from-list)
    (mapcar #'first from-list)))

(defrule distribute-using-from (and kw-distribute dsn-table-name
                                    kw-using maybe-quoted-namestring
                                    kw-from distribute-from-list)
  (:lambda (d-u-f)
    (make-citus-distributed-rule :table (create-table-from-dsn-table-name d-u-f)
                                 :using (make-column :name (fourth d-u-f))
                                 :from (mapcar #'create-table (sixth d-u-f)))))

(defrule distribute-commands (+ (or distribute-using-from
                                    distribute-using
                                    distribute-reference))
  (:lambda (commands)
    (cons :distribute commands)))

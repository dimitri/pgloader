
;;;
;;; ALTER TABLE allows to change some of their properties while migrating
;;; from a source to PostgreSQL, currently only takes care of the schema.
;;;
(in-package #:pgloader.parser)

(defrule match-rule-target-regex quoted-regex)
(defrule match-rule-target-string quoted-namestring
  (:lambda (x) (list :string x)))

(defrule match-rule-target (or match-rule-target-string
                               match-rule-target-regex))

(defrule another-match-rule-target (and comma match-rule-target)
  (:lambda (x)
    (bind (((_ target) x)) target)))

(defrule filter-list-matching
    (and match-rule-target (* another-match-rule-target))
  (:lambda (source)
    (destructuring-bind (filter1 filters) source
      (list* filter1 filters))))

(defrule alter-table-names-matching (and kw-alter kw-table kw-names kw-matching
                                         filter-list-matching)
  (:lambda (alter-table)
    (bind (((_ _ _ _ match-rule-target-list) alter-table))
      match-rule-target-list)))

(defrule rename-to (and kw-rename kw-to quoted-namestring)
  (:lambda (stmt)
    (bind (((_ _ new-name) stmt))
      (list #'pgloader.schema::alter-table-rename new-name))))

(defrule set-schema (and kw-set kw-schema quoted-namestring)
  (:lambda (stmt)
    (bind (((_ _ schema) stmt))
      (list #'pgloader.schema::alter-table-set-schema schema))))

(defrule alter-table-action (or rename-to
                                set-schema))

(defrule alter-table-command (and alter-table-names-matching alter-table-action)
  (:lambda (alter-table-command)
    (destructuring-bind (match-rule-target-list action) alter-table-command
      (loop :for match-rule-target :in match-rule-target-list
         :collect (pgloader.schema::make-match-rule
                   :type   (first match-rule-target)
                   :target (second match-rule-target)
                   :action (first action)
                   :args   (rest action))))))

(defrule alter-table (+ (and alter-table-command ignore-whitespace))
  (:lambda (alter-table-command-list)
    (cons :alter-table
          (loop :for (command ws) :in alter-table-command-list
             :collect command))))

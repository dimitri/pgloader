;;;
;;; MS SQL and SQLite style including/excluding rules, using LIKE
;;;
(in-package #:pgloader.parser)

(defrule like-expression (and "'" (+ (not "'")) "'")
  (:lambda (le)
    (bind (((_ like _) le)) (text like))))

(defrule another-like-expression (and comma like-expression)
  (:lambda (source)
    (bind (((_ like) source)) like)))

(defrule filter-list-like (and like-expression (* another-like-expression))
  (:lambda (source)
    (destructuring-bind (filter1 filters) source
      (list* filter1 filters))))


;;;
;;; Materialize views by copying their data over, allows for doing advanced
;;; ETL processing by having parts of the processing happen on the MySQL
;;; query side.
;;;
(in-package #:pgloader.parser)

(defrule view-name (and (alpha-char-p character)
			(* (or (alpha-char-p character)
			       (digit-char-p character)
			       #\_)))
  (:text t))

(defrule view-sql (and kw-as dollar-quoted)
  (:destructure (as sql) (declare (ignore as)) sql))

(defrule view-definition (and view-name (? view-sql))
  (:destructure (name sql) (cons name sql)))

(defrule another-view-definition (and comma view-definition)
  (:lambda (source)
    (bind (((_ view) source)) view)))

(defrule views-list (and view-definition (* another-view-definition))
  (:lambda (vlist)
    (destructuring-bind (view1 views) vlist
      (list* view1 views))))

(defrule materialize-all-views (and kw-materialize kw-all kw-views)
  (:constant :all))

(defrule materialize-view-list (and kw-materialize kw-views views-list)
  (:destructure (mat views list) (declare (ignore mat views)) list))

(defrule materialize-views (or materialize-view-list materialize-all-views)
  (:lambda (views)
    (cons :views views)))


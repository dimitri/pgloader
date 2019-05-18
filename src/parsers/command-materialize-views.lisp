
;;;
;;; Materialize views by copying their data over, allows for doing advanced
;;; ETL processing by having parts of the processing happen on the MySQL
;;; query side.
;;;
(in-package #:pgloader.parser)

(defrule view-name (or qualified-table-name maybe-quoted-namestring)
  (:lambda (vn)
    (etypecase vn
      (cons   (let* ((schema-name (apply-identifier-case (cdr vn)))
                     (schema (make-schema :source-name (cdr vn)
                                          :name schema-name)))
                (make-matview :source-name vn
                              :name (apply-identifier-case (car vn))
                              :schema schema)))
      (string (make-matview :source-name (cons nil vn)
                            :schema nil
                            :name (apply-identifier-case vn))))))

(defrule view-sql (and kw-as dollar-quoted)
  (:destructure (as sql) (declare (ignore as)) sql))

(defrule view-definition (and view-name (? view-sql))
  (:destructure (matview sql)
    (setf (matview-definition matview) sql)
    matview))

(defrule another-view-definition (and comma-separator view-definition)
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


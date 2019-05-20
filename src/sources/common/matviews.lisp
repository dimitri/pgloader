;;;
;;; Materialized Views support is quite similar from a DB engine from another.
;;;
;; It happens that the view definition is given by the user, so pgloader is
;; not concerned with that part of the SQL compatiblity. The common
;; implementation uses the following two SQL comamnds:
;;;
;;;   CREATE VIEW <schema>.<name> AS <sql>
;;;   DROP VIEW <schema>.<name>, <schema>.<name>, ...;
;;;

(in-package :pgloader.sources)

(defmethod format-matview-name (matview (copy db-copy))
  "Format the materialized view name."
  (declare (ignore copy))
  (let ((schema-name (when (matview-schema matview)
                       (schema-source-name schema)))
        (view-name   (cdr (matview-source-name matview))))
    (format nil "~@[~s.~]~a" schema-name view-name)))

(defmethod create-matviews (matview-list copy)
  "Create Materialized Views as per the pgloader command."
  (unless (eq :all matview-list)
    (let ((views (remove-if #'null matview-list :key #'matview-definition)))
      (when views
        (loop :for mv :in views
           :for sql := (format nil
                               "CREATE VIEW ~a AS ~a"
                               (format-matview-name mv copy)
                               (matview-definition mv))
           :do (progn
                 (log-message :info "SOURCE: ~a;" sql)
                 #+pgloader-image
                 (query (source-db copy) sql)
                 #-pgloader-image
                 (restart-case
                     (query (source-db copy) sql)
                   (use-existing-view ()
                     :report "Use the already existing view and continue"
                     nil)
                   (replace-view ()
                     :report
                     "Replace the view with the one from pgloader's command"
                     (let ((drop-sql (format nil "DROP VIEW ~a"
                                             (format-matview-name mv copy))))
                       (log-message :info "SOURCE: ~a;" drop-sql)
                       ;; drop the materialized view, then create it again
                       (query (source-db copy) drop-sql)
                       (query (source-db copy) sql))))))))))

(defmethod drop-matviews (matview-list copy)
  "Drop Materialized Views created just for the pgloader migration."
  (unless (eq :all matview-list)
    (let ((views (remove-if #'null matview-list :key #'matview-definition)))
      (when views
        (let ((sql (format nil "DROP VIEW ~{~a~^, ~}"
                           (mapcar (lambda (mv) (format-matview-name mv copy))
                                   views))))
          (log-message :info "SOURCE: ~a;" sql)
          (query (source-db copy) sql))))))

(in-package :pgloader.source.pgsql)

(defun create-pg-views (views-alist)
  "VIEWS-ALIST associates view names with their SQL definition, which might
   be empty for already existing views. Create only the views for which we
   have an SQL definition."
  (unless (eq :all views-alist)
    (let ((views (remove-if #'null views-alist :key #'cdr)))
      (when views
        (loop :for (name . def) :in views
           :for sql := (destructuring-bind (schema . v-name) name
                         (format nil
                                 "CREATE VIEW ~@[~s.~]~s AS ~a"
                                 schema v-name def))
           :do (progn
                 (log-message :info "PostgreSQL Source: ~a" sql)
                 #+pgloader-image
                 (pgsql-execute sql)
                 #-pgloader-image
                 (restart-case
                     (pgsql-execute sql)
                   (use-existing-view ()
                     :report "Use the already existing view and continue"
                     nil)
                   (replace-view ()
                     :report
                     "Replace the view with the one from pgloader's command"
                     (let ((drop-sql (format nil "DROP VIEW ~a;" (car name))))
                       (log-message :info "PostgreSQL Source: ~a" drop-sql)
                       (pgsql-execute drop-sql)
                       (pgsql-execute sql))))))))))

(defun drop-pg-views (views-alist)
  "See `create-pg-views' for VIEWS-ALIST description. This time we DROP the
   views to clean out after our work."
  (unless (eq :all views-alist)
   (let ((views (remove-if #'null views-alist :key #'cdr)))
     (when views
       (let ((sql
              (with-output-to-string (sql)
                (format sql "DROP VIEW ")
                (loop :for view-definition :in views
                   :for i :from 0
                   :do (destructuring-bind (name . def) view-definition
                         (declare (ignore def))
                         (format sql
                                 "~@[, ~]~@[~s.~]~s"
                                 (not (zerop i)) (car name) (cdr name)))))))
         (log-message :info "PostgreSQL Source: ~a" sql)
         (pgsql-execute sql))))))

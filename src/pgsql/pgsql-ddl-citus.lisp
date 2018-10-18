;;;
;;; PostgreSQL Citus support for calling functions.
;;;

(in-package :pgloader.pgsql)

(defmethod format-create-sql ((rule citus-reference-rule)
                              &key (stream nil) if-not-exists)
  (declare (ignore if-not-exists))
  (format stream "SELECT create_reference_table('~a');"
          (format-table-name (citus-reference-rule-table rule))))

(defmethod format-create-sql ((rule citus-distributed-rule)
                              &key (stream nil) if-not-exists)
  (declare (ignore if-not-exists))
  (format stream "SELECT create_distributed_table('~a', '~a');"
          (format-table-name (citus-distributed-rule-table rule))
          (column-name (citus-distributed-rule-using rule))))

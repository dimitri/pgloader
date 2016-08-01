;;;
;;; API to rewrite index WHERE clauses (filter)
;;;
(in-package #:pgloader.pgsql)

(defgeneric translate-index-filter (table index sql-dialect)
  (:documentation
   "Translate the filter clause of INDEX in PostgreSQL slang."))

(defmethod translate-index-filter ((table table)
                                   (index index)
                                   (sql-dialect t))
  "Implement a default facility that does nothing."
  nil)


;;;
;;; Generic code that drives all index filter clauses rewriting.
;;;

(defgeneric process-index-definitions (object &key sql-dialect)
  (:documentation "Rewrite all indexes filters in given catalog OBJECT."))

(defmethod process-index-definitions ((catalog catalog) &key sql-dialect)
  "Rewrite all index filters in CATALOG."
  (loop :for schema :in (catalog-schema-list catalog)
     :do (process-index-definitions schema :sql-dialect sql-dialect)))

(defmethod process-index-definitions ((schema schema) &key sql-dialect)
  "Rewrite all index filters in CATALOG."
  (loop :for table :in (schema-table-list schema)
     :do (process-index-definitions table :sql-dialect sql-dialect)))

(defmethod process-index-definitions ((table table) &key sql-dialect)
  "Rewrite all index filter in TABLE."
  (loop :for index :in (table-index-list table)
     :when (index-filter index)
     :do (let ((pg-filter
                (handler-case
                    (translate-index-filter table index sql-dialect)
                  (condition (c)
                    (log-message :error
                                 "Failed to translate index ~s on table ~s because of filter clause ~s"
                                 (index-name index)
                                 (format-table-name table)
                                 (index-filter index))
                    (log-message :debug "filter translation error: ~a" c)
                    ;; try to create the index without the WHERE clause...
                    (setf (index-filter index) nil)))))
           (log-message :info "tranlate-index-filter: ~s" pg-filter)
           (setf (index-filter index) pg-filter))))

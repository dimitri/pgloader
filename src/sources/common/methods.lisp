;;;
;;; Common methods implementations, default behaviour.
;;;

(in-package #:pgloader.sources)

(defmethod data-is-preformatted-p ((copy copy))
  "By default, data is not preformatted."
  nil)

(defmethod preprocess-row ((copy copy))
  "The default preprocessing of raw data is to do nothing."
  nil)

(defmethod copy-column-list ((copy copy))
  "Default column list is an empty list."
  nil)

(defmethod cleanup ((copy db-copy) (catalog catalog) &key materialize-views)
  "In case anything wrong happens at `prepare-pgsql-database' step, this
  function will be called to clean-up the mess left behind, if any."
  (declare (ignorable materialize-views))
  t)

(defmethod instanciate-table-copy-object ((copy db-copy) (table table))
  "Create an new instance for copying TABLE data."
  (let* ((fields     (table-field-list table))
         (columns    (table-column-list table))
         (transforms (mapcar #'column-transform columns)))
    (make-instance (class-of copy)
                   :source-db  (clone-connection (source-db copy))
                   :target-db  (clone-connection (target-db copy))
                   :source     table
                   :target     table
                   :fields     fields
                   :columns    columns
                   :transforms transforms)))

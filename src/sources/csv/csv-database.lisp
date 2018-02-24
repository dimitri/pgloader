;;;
;;; Experimental code, used to be used a long time ago, before this lisp
;;; code became pgloader. The idea is to use it again sometimes, someway.
;;;
(in-package #:pgloader.source.csv)

;;;
;;; When you exported a whole database as a bunch of CSV files to be found
;;; in the same directory, each file name being the name of the target
;;; table, then this function allows to import them all at once.
;;;
;;; TODO: expose it from the command language, and test it.
;;;
(defun import-database (dbname
			&key
			  (fd-path-root *fd-path-root*)
			  (skip-lines 0)
			  (separator #\Tab)
			  (quote cl-csv:*quote*)
			  (escape cl-csv:*quote-escape*)
			  (truncate t)
			  only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  (let ((*state* (pgloader.utils:make-pgstate)))
    (report-header)
    (loop
       for (table-name . date-columns) in (pgloader.pgsql:list-tables dbname)
       for filename = (get-pathname dbname table-name
				    :fd-path-root fd-path-root)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
         (let ((source (make-instance 'copy-csv
                                      :target-db  dbname
                                      :source     (list :filename filename)
                                      :target     table-name
                                      :skip-lines skip-lines
                                      :separator separator
                                      :quote quote
                                      :escape escape)))
           (copy-from source :truncate truncate))
       finally
	 (report-pgstate-stats *state* "Total import time"))))

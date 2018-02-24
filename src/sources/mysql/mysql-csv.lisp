;;;
;;; Tools to handle MySQL data fetching: old code, untested, kept for reference.
;;;

(in-package :pgloader.source.mysql)

;;;
;;; MySQL bulk export to file, in PostgreSQL COPY TEXT format
;;;
(defun export-database (dbname &key only-tables)
  "Export MySQL tables into as many TEXT files, in the PostgreSQL COPY format"
  (let ((all-columns  (list-all-columns :dbname dbname)))
    (setf *state* (pgloader.utils:make-pgstate))
    (report-header)
    (loop
       for (table-name . cols) in all-columns
       for filename = (pgloader.csv:get-pathname dbname table-name)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgstate-add-table *state* dbname table-name)
	 (report-table-name table-name)
	 (multiple-value-bind (rows secs)
	     (timing
	      ;; load data
	      (let ((source
		     (make-instance 'copy-mysql
				    :source-db dbname
				    :source table-name
				    :fields cols)))
		(copy-to source filename)))
	   ;; update and report stats
	   (pgstate-incf *state* table-name :read rows :secs secs)
	   (report-pgtable-stats *state* table-name))
       finally
	 (report-pgstate-stats *state* "Total export time"))))

;;;
;;; Copy data from a target database into files in the PostgreSQL COPY TEXT
;;; format, then load those files. Useful mainly to compare timing with the
;;; direct streaming method. If you need to pre-process the files, use
;;; export-database, do the extra processing, then use
;;; pgloader.pgsql:copy-from-file on each file.
;;;
(defun export-import-database (dbname
			       &key
				 (pg-dbname dbname)
				 (truncate t)
				 only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  ;; get the list of tables and have at it
  (let ((all-columns  (list-all-columns :dbname dbname)))
    (setf *state* (pgloader.utils:make-pgstate))
    (report-header)
    (loop
       for (table-name . cols) in all-columns
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (pgstate-add-table *state* dbname table-name)
	 (report-table-name table-name)

	 (multiple-value-bind (res secs)
	     (timing
	      (let* ((filename (pgloader.csv:get-pathname dbname table-name)))
		;; export from MySQL to file
		(let ((source
		       (make-instance 'copy-mysql
				      :source-db dbname
				      :source table-name
				      :fields cols)))
		  (copy-to source filename))
		;; import the file to PostgreSQL
		(pgloader.pgsql:copy-from-file pg-dbname
					       table-name
					       filename
					       :truncate truncate)))
	   (declare (ignore res))
	   (pgstate-incf *state* table-name :secs secs)
	   (report-pgtable-stats *state* table-name))
       finally
	 (report-pgstate-stats *state* "Total export+import time"))))

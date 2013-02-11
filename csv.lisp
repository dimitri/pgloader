;;;
;;; Tools to handle MySQL data fetching
;;;

(in-package :pgloader.csv)

(defun get-pathname (dbname table-name &key (csv-path-root *csv-path-root*))
  "Return a pathname where to read or write the file data"
  (make-pathname
   :directory (pathname-directory
	       (merge-pathnames (format nil "~a/" dbname) csv-path-root))
   :name table-name
   :type "csv"))

;;;
;;; Read a file format in CSV format, and call given function on each line.
;;;
(defun map-rows (table-name filename
		 &key
		   process-row-fn
		   (skip-first-p nil)
		   (separator #\Tab)
		   (quote cl-csv:*quote*)
		   (escape cl-csv:*quote-escape*)
		   (null-as "\\N"))
  "Load data from a text file in PostgreSQL COPY TEXT format.

Each row is pre-processed then PROCESS-ROW-FN is called with the row as a
list as its only parameter.

Finally returns how many rows where read and processed."
  (with-open-file
      ;; we just ignore files that don't exist
      (input filename
	     :direction :input
	     :external-format :utf8
	     :if-does-not-exist nil)
    (when input
      ;; read in the text file, split it into columns, process NULL columns
      ;; the way postmodern expects them, and call PROCESS-ROW-FN on them

      (let* ((read 0)
	     (reformat-then-process
	      (lambda (row)
		(incf read)
		(let ((row-with-nils
		       (mapcar (lambda (x) (if (string= null-as x) nil x)) row)))
		  (funcall process-row-fn row-with-nils)))))

	(handler-case
	    (cl-csv:read-csv input
			     :row-fn reformat-then-process
			     :skip-first-p skip-first-p
			     :separator separator
			     :quote quote
			     :escape escape)
	  ((or cl-csv:csv-parse-error type-error) (condition)
	    ;; some form of parse error did happen, TODO: log it
	   (pgstate-setf *state* table-name :errs -1)))
	;; return how many rows we did read
	read))))

(defun copy-to-queue (table-name filename dataq
		      &key
			(skip-first-p nil)
			(separator #\Tab)
			(quote cl-csv:*quote*)
			(escape cl-csv:*quote-escape*)
			(null-as "\\N"))
  "Copy data from CSV FILENAME into lprallel.queue DATAQ"
  (let ((read
	 (pgloader.queue:map-push-queue dataq #'map-rows table-name filename
					:skip-first-p skip-first-p
					:separator separator
					:quote quote
					:escape escape
					:null-as null-as)))
    (pgstate-incf *state* table-name :read read)))

(defun copy-from-file (dbname table-name filename
		       &key
			 (truncate t)
			 date-columns
			 (skip-first-p nil)
			 (separator #\Tab)
			 (quote cl-csv:*quote*)
			 (escape cl-csv:*quote-escape*)
			 (null-as "\\N"))
  "Copy data from CSV file FILENAME into PostgreSQL DBNAME.TABLE-NAME"
  (let* ((lp:*kernel* *loader-kernel*)
	 (channel     (lp:make-channel))
	 (dataq       (lq:make-queue 4096)))
    (lp:submit-task channel (lambda ()
			      ;; this function update :read stats
			      (copy-to-queue table-name filename dataq
					     :skip-first-p skip-first-p
					     :separator separator
					     :quote quote
					     :escape escape
					     :null-as null-as)))
    ;; and start another task to push that data from the queue to PostgreSQL
    (lp:submit-task
     channel
     (lambda ()
       ;; this function update :rows stats
       (pgloader.pgsql:copy-from-queue dbname table-name dataq
				       :truncate truncate
				       :date-columns date-columns)))

    ;; now wait until both the tasks are over
    (loop for tasks below 2 do (lp:receive-result channel))))

(defun import-database (dbname
			&key
			  (csv-path-root *csv-path-root*)
			  (skip-first-p nil)
			  (separator #\Tab)
			  (quote cl-csv:*quote*)
			  (escape cl-csv:*quote-escape*)
			  (null-as "\\N")
			  (truncate t)
			  only-tables)
  "Export MySQL data and Import it into PostgreSQL"
  (setf *state* (pgloader.utils:make-pgstate))
  (report-header)
  (loop
     for (table-name . date-columns) in (pgloader.pgsql:list-tables dbname)
     for filename = (get-pathname dbname table-name
				  :csv-path-root csv-path-root)
     when (or (null only-tables)
	      (member table-name only-tables :test #'equal))
     do
       (pgstate-add-table *state* dbname table-name)
       (report-table-name table-name)

       (multiple-value-bind (res secs)
	   (timing
	    ;; this will care about updating stats in *state*
	    (copy-from-file dbname table-name filename
			    :skip-first-p skip-first-p
			    :separator separator
			    :quote quote
			    :escape escape
			    :null-as null-as
			    :truncate truncate
			    :date-columns date-columns))
	 ;; set the timing we just measured
	 (declare (ignore res))
	 (pgstate-incf *state* table-name :secs secs)
	 (report-pgtable-stats *state* table-name))

     finally
       (report-pgstate-stats *state* "Total import time")))

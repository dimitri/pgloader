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

(defun get-absolute-pathname (pathname-or-regex &key (root *csv-path-root*))
  "PATHNAME-OR-REGEX is expected to be either (:regexp expression)
   or (:filename pathname). In the first case, this fonction check if the
   pathname is absolute or relative and returns an absolute pathname given
   current working directory of ROOT.

   In the second case, walk the ROOT directory and return the first pathname
   that matches the regex. TODO: consider signaling a condition when we have
   more than one match."
  (destructuring-bind (type part) pathname-or-regex
    (ecase type
      (:regex    (first (pgloader.archive:get-matching-filenames root part)))
      (:filename (if (fad:pathname-absolute-p part) part
		     (merge-pathnames part root))))))

;;;
;;; Project fields into columns
;;;
(defun project-fields (&key fields columns null-as)
  "The simplest projection happens when both FIELDS and COLS are nil: in
   this case the projection is an identity, we simply return what we got --
   still with some basic processing.

   Other forms of projections consist of forming columns with the result of
   applying a transformation function. In that case a cols entry is a list
   of '(colname type expression), the expression being the (already
   compiled) function to use here."

  (let ((null-as (if (eq null-as :blanks)
		     (lambda (col)
		       (every (lambda (char) (char= char #\Space)) col))
		     `(lambda (col)
			(if (string= ,null-as col) nil col)))))

    (if (or (null fields) (null columns))
	`(lambda (row)
	   (mapcar ,null-as row))

	(let* ((args
		(mapcar
		 (lambda (field)
		   (pgloader.transforms:intern-symbol (car field))) fields))
	       (newrow
		(loop for (name type fn) in columns
		   collect (or fn
			       (let ((binding
				      (pgloader.transforms:intern-symbol name)))
				 `(funcall ,null-as ,binding))))))
	  `(lambda (row)
	     (destructuring-bind (,@args) row
	       (list ,@newrow)))))))

;;;
;;; Read a file format in CSV format, and call given function on each line.
;;;
(defun map-rows (table-name filename
		 &key
		   process-row-fn
		   fields
		   columns
		   (skip-lines nil)
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
	     :external-format :utf-8
	     :if-does-not-exist nil)
    (when input
      ;; read in the text file, split it into columns, process NULL columns
      ;; the way postmodern expects them, and call PROCESS-ROW-FN on them

      (let* ((read 0)
	     (reformat-then-process
	      (compile nil
		       (lambda (row)
			 (incf read)
			 (let* ((processing (project-fields :fields fields
							    :columns columns
							    :null-as null-as))
				(processed-row (funcall processing row)))
			   (funcall process-row-fn processed-row))))))

	;; we handle skipping more than one line here, as cl-csv only knows
	;; about skipping the first line
	(when (and skip-lines (< 0 skip-lines))
	  (loop repeat skip-lines do (read-line input nil nil)))

	(handler-case
	    (cl-csv:read-csv input
			     :row-fn reformat-then-process
			     :separator separator
			     :quote quote
			     :escape escape)
	  ((or cl-csv:csv-parse-error type-error) (condition)
	    ;; some form of parse error did happen, TODO: log it
	    (format t "~&~a~%" condition)
	   (pgstate-setf *state* table-name :errs -1)))
	;; return how many rows we did read
	read))))

(defun copy-to-queue (table-name filename dataq
		      &key
			fields
			columns
			skip-lines
			(separator #\Tab)
			(quote cl-csv:*quote*)
			(escape cl-csv:*quote-escape*)
			(null-as "\\N"))
  "Copy data from CSV FILENAME into lprallel.queue DATAQ"
  (let ((read
	 (pgloader.queue:map-push-queue dataq #'map-rows table-name filename
					:fields fields
					:columns columns
					:skip-lines skip-lines
					:separator separator
					:quote quote
					:escape escape
					:null-as null-as)))
    (pgstate-incf *state* table-name :read read)))

(defun copy-from-file (dbname table-name filename-or-regex
		       &key
			 fields
			 columns
			 transforms
			 (truncate t)
			 skip-lines
			 (separator #\Tab)
			 (quote cl-csv:*quote*)
			 (escape cl-csv:*quote-escape*)
			 (null-as "\\N"))
  "Copy data from CSV file FILENAME into PostgreSQL DBNAME.TABLE-NAME"
  (let* ((report-header  (null *state*))
	 (*state*        (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel*    (make-kernel 2))
	 (channel        (lp:make-channel))
	 (dataq          (lq:make-queue :fixed-capacity 4096))
	 (filename       (get-absolute-pathname filename-or-regex)))

    ;; statistics
    (when report-header (report-header))
    (pgstate-add-table *state* dbname table-name)
    (report-table-name table-name)

    (multiple-value-bind (res secs)
	(timing
	 (lp:submit-task channel
			 ;; this function update :read stats
			 #'copy-to-queue table-name filename dataq
			 :fields fields
			 :columns columns
			 :skip-lines skip-lines
			 :separator separator
			 :quote quote
			 :escape escape
			 :null-as null-as)

	 ;; and start another task to push that data from the queue to PostgreSQL
	 (lp:submit-task channel
			 ;; this function update :rows stats
			 #'pgloader.pgsql:copy-from-queue dbname table-name dataq
			 :truncate truncate
			 :transforms transforms)

	 ;; now wait until both the tasks are over
	 (loop for tasks below 2 do (lp:receive-result channel))
	 (lp:end-kernel))

      ;; report stats!
      (declare (ignore res))
      (pgstate-incf *state* table-name :secs secs)
      (report-pgtable-stats *state* table-name))))

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
  (let ((*state* (pgloader.utils:make-pgstate)))
    (report-header)
    (loop
       for (table-name . date-columns) in (pgloader.pgsql:list-tables dbname)
       for filename = (get-pathname dbname table-name
				    :csv-path-root csv-path-root)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (copy-from-file dbname table-name filename
			 :skip-first-p skip-first-p
			 :separator separator
			 :quote quote
			 :escape escape
			 :null-as null-as
			 :truncate truncate)
       finally
	 (report-pgstate-stats *state* "Total import time"))))

;;;
;;; Automatic guess the CSV format parameters
;;;
(defparameter *separators* '(#\Tab #\, #\; #\| #\% #\^ #\! #\$)
  "Common CSV separators to try when guessing file parameters.")

(defparameter *escape-quotes* '("\\\"" "\"\"")
  "Common CSV quotes to try when guessing file parameters.")

(defun get-file-sample (filename &key (sample-size 10))
  "Return the first SAMPLE-SIZE lines in FILENAME (or less), or nil if the
   file does not exists."
  (with-open-file
      ;; we just ignore files that don't exist
      (input filename
	     :direction :input
	     :external-format :utf-8
	     :if-does-not-exist nil)
    (when input
      (loop
	 for line = (read-line input nil)
	 while line
	 repeat sample-size
	 collect line))))

(defun try-csv-params (lines cols &key separator quote escape)
  "Read LINES as CSV with SEPARATOR and ESCAPE params, and return T when
   each line in LINES then contains exactly COLS columns"
  (let ((rows (loop
		 for line in lines
		 append
		   (handler-case
		       (cl-csv:read-csv line
					:quote quote
					:separator separator
					:escape escape)
		     ((or cl-csv:csv-parse-error type-error) ()
		       nil)))))
    (and rows
	 (every (lambda (row) (= cols (length row))) rows))))

(defun guess-csv-params (filename cols &key (sample-size 10))
  "Try a bunch of field separators with LINES and return the first one that
   returns COLS number of columns"

  (let ((sample (get-file-sample filename :sample-size sample-size)))
    (loop
       for sep in *separators*
       for esc = (loop
		    for escape in *escape-quotes*
		    when (try-csv-params sample cols
					 :quote #\"
					 :separator sep
					 :escape escape)
		    do (return escape))
       when esc
       do (return (list :separator sep :quote #\" :escape esc)))))

(defun guess-all-csv-params (dbname)
  "Return a list of table-name and CSV parameters for tables in PostgreSQL
   database DBNAME."
  (loop
     for (table-name . cols) in (pgloader.pgsql:list-tables-cols dbname)
     for filename = (get-pathname dbname table-name)
     collect (cons table-name (guess-csv-params filename cols))))

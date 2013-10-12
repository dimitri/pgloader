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
      (:inline   part)
      (:stdin    *standard-input*)
      (:regex    (first (pgloader.archive:get-matching-filenames root part)))
      (:filename (if (fad:pathname-absolute-p part) part
		     (merge-pathnames part root))))))

;;;
;;; Project fields into columns
;;;
(defun project-fields (&key fields columns (compile t))
  "The simplest projection happens when both FIELDS and COLS are nil: in
   this case the projection is an identity, we simply return what we got.

   Other forms of projections consist of forming columns with the result of
   applying a transformation function. In that case a cols entry is a list
   of '(colname type expression), the expression being the (already
   compiled) function to use here."
  (labels ((null-as-processing-fn (null-as)
	     "return a lambda form that will process a value given NULL-AS."
	     (if (eq null-as :blanks)
		 (lambda (col)
		   (declare (optimize speed))
		   (if (every (lambda (char) (char= char #\Space)) col)
		       nil
		       col))
		 (lambda (col)
		   (declare (optimize speed))
		   (if (string= null-as col) nil col))))

	   (field-name-as-symbol (field-name-or-list)
	     "we need to deal with symbols as we generate code"
	     (typecase field-name-or-list
	       (list (pgloader.transforms:intern-symbol (car field-name-or-list)))
	       (t    (pgloader.transforms:intern-symbol field-name-or-list))))

	   (field-process-null-fn (field-name-or-list)
	     "Given a field entry, return a function dealing with nulls for it"
	     (destructuring-bind (&key null-as date-format)
		 (typecase field-name-or-list
		   (list (cdr field-name-or-list))
		   (t    (cdr (assoc field-name-or-list fields :test #'string=))))
	       (declare (ignore date-format)) ; TODO
	       (if (null null-as)
		   #'identity
		   (null-as-processing-fn null-as)))))

    (let* ((projection
	    (cond
	      ;; when no specific information has been given on FIELDS and
	      ;; COLUMNS, just apply generic NULL-AS processing
	      ((and (null fields) (null columns))
	       (lambda (row) row))

	      ((null columns)
	       ;; when no specific information has been given on COLUMNS,
	       ;; use the information given for FIELDS and apply per-field
	       ;; null-as, or the generic one if none has been given for
	       ;; that field.
	       (let ((process-nulls
		      (mapcar (function field-process-null-fn) fields)))
		 `(lambda (row)
		   (loop
		      :for col :in row
		      :for fn :in ',process-nulls
		      :collect (funcall fn col)))))

	      (t
	       ;; project some number of FIELDS into a possibly different
	       ;; number of COLUMNS, using given transformation functions,
	       ;; processing NULL-AS represented values.
	       (let* ((args
		       (if fields
			   (mapcar (function field-name-as-symbol) fields)
			   (mapcar (function field-name-as-symbol) columns)))
		      (newrow
		       (loop for (name type fn) in columns
			  collect
			  ;; we expect the name of a COLUMN to be the same
			  ;; as the name of its derived FIELD when we
			  ;; don't have any transformation function
			    (or fn `(funcall ,(field-process-null-fn name)
					     ,(field-name-as-symbol name))))))
		 `(lambda (row)
		    (declare (optimize speed) (type list row))
		    (destructuring-bind (,@args) row
		      (list ,@newrow))))))))
      ;; allow for some debugging
      (if compile (compile nil projection) projection))))

;;;
;;; Read a file format in CSV format, and call given function on each line.
;;;
(defun map-rows (table-name filespec
		 &key
		   process-row-fn
		   fields
		   columns
		   (encoding :utf-8)
		   (skip-lines nil)
		   (separator #\Tab)
		   (quote cl-csv:*quote*)
		   (escape cl-csv:*quote-escape*)
		   (trim-blanks cl-csv:*trim-blanks*))
  "Load data from a text file in CSV format, with support for advanced
   projecting capabilities. See `project-fields' for details.

   Each row is pre-processed then PROCESS-ROW-FN is called with the row as a
   list as its only parameter.

   FILESPEC is either a filename or a pair (filename . position) where
   position is the number of bytes to skip in the file before getting to the
   data. That's used to handle the INLINE data loading.

   Finally returns how many rows where read and processed."
  (let ((filename (if (consp filespec) (car filespec) filespec)))
   (with-open-file
       ;; we just ignore files that don't exist
       (input filename
	      :direction :input
	      :external-format encoding
	      :if-does-not-exist nil)
     (when input
       ;; first go to given inline position when filename is a consp
       (when (consp filespec)
	 (loop repeat (cdr filespec) do (read-char input)))

       ;; we handle skipping more than one line here, as cl-csv only knows
       ;; about skipping the first line
       (when (and skip-lines (< 0 skip-lines))
	 (loop repeat skip-lines do (read-line input nil nil)))

       ;; read in the text file, split it into columns, process NULL columns
       ;; the way postmodern expects them, and call PROCESS-ROW-FN on them
       (let* ((read 0)
	      (projection (project-fields :fields fields
					  :columns columns))
	      (reformat-then-process
	       (lambda (row)
		 (incf read)
		 (let ((projected-row
			(handler-case
			    (funcall projection row)
			  (condition (e)
			    (pgstate-incf *state* table-name :errs 1)
			    (log-message :error
					 "Could not read line ~d: ~a" read e)))))
		   (when projected-row
		     (funcall process-row-fn projected-row))))))

	 (handler-case
	     (cl-csv:read-csv input
			      :row-fn (compile nil reformat-then-process)
			      :separator separator
			      :quote quote
			      :escape escape
			      :trim-blanks trim-blanks)
	   ((or cl-csv:csv-parse-error type-error) (condition)
	     ;; some form of parse error did happen, TODO: log it
	     (progn
	       (log-message :error "~a" condition)
	       (pgstate-setf *state* table-name :errs -1))))
	 ;; return how many rows we did read
	 read)))))

(defun copy-to-queue (table-name filename dataq
		      &key
			fields
			columns
			encoding
			skip-lines
			(separator #\Tab)
			(quote cl-csv:*quote*)
			(escape cl-csv:*quote-escape*)
			(trim-blanks cl-csv:*trim-blanks*))
  "Copy data from CSV FILENAME into lprallel.queue DATAQ"
  (let ((read
	 (pgloader.queue:map-push-queue dataq
					#'map-rows
					table-name filename
					:fields fields
					:columns columns
					:encoding encoding
					:skip-lines skip-lines
					:separator separator
					:quote quote
					:escape escape
					:trim-blanks trim-blanks)))
    (pgstate-incf *state* table-name :read read)))

(defun copy-from-file (dbname table-name filename-or-regex
		       &key
			 fields
			 columns
			 (transforms
			  (loop for c
			     in (or columns
				    (pgloader.pgsql:list-columns dbname table-name))
			     collect nil))
			 truncate
			 skip-lines
			 (encoding :utf-8)
			 (separator #\Tab)
			 (quote cl-csv:*quote*)
			 (escape cl-csv:*quote-escape*)
			 (trim-blanks cl-csv:*trim-blanks*))
  "Copy data from CSV file FILENAME into PostgreSQL DBNAME.TABLE-NAME"
  (let* ((summary        (null *state*))
	 (*state*        (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel*    (make-kernel 2))
	 (channel        (lp:make-channel))
	 (dataq          (lq:make-queue :fixed-capacity 4096))
	 (filename       (get-absolute-pathname filename-or-regex)))

    (with-stats-collection (dbname table-name :state *state* :summary summary)
      (log-message :notice "COPY ~a.~a" dbname table-name)
      (lp:submit-task channel
		      ;; this function update :read stats
		      #'pgloader.csv:copy-to-queue table-name filename dataq
		      :fields fields
		      :columns columns
		      :encoding encoding
		      :skip-lines skip-lines
		      :separator separator
		      :quote quote
		      :escape escape
		      :trim-blanks trim-blanks)

      ;; and start another task to push that data from the queue to PostgreSQL
      (lp:submit-task channel
		      ;; this function update :rows stats
		      #'pgloader.pgsql:copy-from-queue dbname table-name dataq
		      ;; we only are interested into the column names here
		      :columns (when columns (mapcar #'car columns))
		      :truncate truncate
		      :transforms transforms)

      ;; now wait until both the tasks are over
      (loop for tasks below 2 do (lp:receive-result channel)
	 finally (lp:end-kernel)))))

(defun import-database (dbname
			&key
			  (csv-path-root *csv-path-root*)
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
				    :csv-path-root csv-path-root)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (copy-from-file dbname table-name filename
			 :skip-lines skip-lines
			 :separator separator
			 :quote quote
			 :escape escape
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

;;;
;;; Tools to handle MySQL data fetching
;;;

(in-package :pgloader.csv)

;;;
;;; Implementing the pgloader source API
;;;
(defclass copy-csv (copy)
  ((source-type :accessor source-type	  ; one of :inline, :stdin, :regex
		:initarg :source-type)	  ;  or :filename
   (encoding    :accessor encoding	  ; file encoding
	        :initarg :encoding)	  ;
   (skip-lines  :accessor skip-lines	  ; CSV headers
	        :initarg :skip-lines	  ;
		:initform 0)		  ;
   (separator   :accessor csv-separator	  ; CSV separator
	        :initarg :separator	  ;
	        :initform #\Tab)	  ;
   (quote       :accessor csv-quote	  ; CSV quoting
	        :initarg :quote		  ;
	        :initform cl-csv:*quote*) ;
   (escape      :accessor csv-escape	  ; CSV quote escaping
	        :initarg :escape	  ;
	        :initform cl-csv:*quote-escape*)
   (trim-blanks :accessor csv-trim-blanks ; CSV blank and NULLs
		:initarg :trim-blanks	  ;
		:initform t))
  (:documentation "pgloader CSV Data Source"))

(defmethod initialize-instance :after ((csv copy-csv) &key)
  "Compute the real source definition from the given source parameter, and
   set the transforms function list as needed too."
  (let ((source (slot-value csv 'source)))
    (setf (slot-value csv 'source-type) (car source))
    (setf (slot-value csv 'source)      (get-absolute-pathname source)))

  (let ((transforms (when (slot-boundp csv 'transforms)
		      (slot-value csv 'transforms)))
	(columns
	 (or (slot-value csv 'columns)
	     (pgloader.pgsql:list-columns (slot-value csv 'target-db)
					  (slot-value csv 'target)))))
    (unless transforms
      (setf (slot-value csv 'transforms)
	    (loop for c in columns collect nil)))))

;;;
;;; Read a file format in CSV format, and call given function on each line.
;;;
(defmethod map-rows ((csv copy-csv) &key process-row-fn)
  "Load data from a text file in CSV format, with support for advanced
   projecting capabilities. See `project-fields' for details.

   Each row is pre-processed then PROCESS-ROW-FN is called with the row as a
   list as its only parameter.

   FILESPEC is either a filename or a pair (filename . position) where
   position is the number of bytes to skip in the file before getting to the
   data. That's used to handle the INLINE data loading.

   Finally returns how many rows where read and processed."
  (let ((filenames   (case (source-type csv)
		       (:inline  (list (car (source csv))))
		       (:regex   (source csv))
		       (t        (list (source csv))))))
    (loop for filename in filenames
       do
	 (with-open-file
	     ;; we just ignore files that don't exist
	     (input filename
		    :direction :input
		    :external-format (encoding csv)
		    :if-does-not-exist nil)
	   (when input
	     (log-message :info "COPY FROM ~s" filename)

	     ;; first go to given inline position when filename is :inline
	     (when (eq (source-type csv) :inline)
	       (file-position input (cdr (source csv))))

	     ;; we handle skipping more than one line here, as cl-csv only knows
	     ;; about skipping the first line
	     (loop repeat (skip-lines csv) do (read-line input nil nil))

	     ;; read in the text file, split it into columns, process NULL
	     ;; columns the way postmodern expects them, and call
	     ;; PROCESS-ROW-FN on them
	     (let ((reformat-then-process
		    (reformat-then-process :fields  (fields csv)
					   :columns (columns csv)
					   :target  (target csv)
					   :process-row-fn process-row-fn)))
	       (handler-case
		   (cl-csv:read-csv input
				    :row-fn (compile nil reformat-then-process)
				    :separator (csv-separator csv)
				    :quote (csv-quote csv)
				    :escape (csv-escape csv)
                                    :unquoted-empty-string-is-nil t
                                    :quoted-empty-string-is-nil nil
				    :trim-outer-whitespace (csv-trim-blanks csv))
		 ((or cl-csv:csv-parse-error type-error) (condition)
		   (progn
		     (log-message :error "~a" condition)
		     (pgstate-setf *state* (target csv) :errs -1))))))))))

(defmethod copy-to-queue ((csv copy-csv) queue)
  "Copy data from given CSV definition into lparallel.queue DATAQ"
  (map-push-queue csv queue))

(defmethod copy-from ((csv copy-csv) &key truncate)
  "Copy data from given CSV file definition into its PostgreSQL target table."
  (let* ((summary        (null *state*))
	 (*state*        (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel*    (make-kernel 2))
	 (channel        (lp:make-channel))
	 (queue          (lq:make-queue :fixed-capacity *concurrent-batches*))
	 (dbname         (target-db csv))
	 (table-name     (target csv)))

    (with-stats-collection (table-name :state *state* :summary summary)
      (log-message :notice "COPY ~a.~a" dbname table-name)
      (lp:submit-task channel #'copy-to-queue csv queue)

      ;; and start another task to push that data from the queue to PostgreSQL
      (lp:submit-task channel
		      ;; this function update :rows stats
		      #'pgloader.pgsql:copy-from-queue dbname table-name queue
		      ;; we only are interested into the column names here
		      :columns (mapcar #'car (columns csv))
		      :truncate truncate)

      ;; now wait until both the tasks are over
      (loop for tasks below 2 do (lp:receive-result channel)
	 finally (lp:end-kernel)))))

;;;
;;; When you exported a whole database as a bunch of CSV files to be found
;;; in the same directory, each file name being the name of the target
;;; table, then this function allows to import them all at once.
;;;
;;; TODO: expose it from the command language, and test it.
;;;
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


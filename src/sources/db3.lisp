;;;
;;; Tools to handle the DBF file format
;;;

(in-package :pgloader.db3)

(defvar *db3-pgsql-type-mapping*
  '(("C" . "text")			; ignore field-length
    ("N" . "numeric")			; handle both integers and floats
    ("L" . "boolean")			; PostgreSQL compatible representation
    ("D" . "date")			; no TimeZone in DB3 files
    ("M" . "text")))			; not handled yet

(defstruct (db3-field
	     (:constructor make-db3-field (name type length)))
  name type length)

(defmethod format-pgsql-column ((col db3-field) &key identifier-case)
  "Return a string representing the PostgreSQL column definition."
  (let* ((column-name
	  (apply-identifier-case (db3-field-name col) identifier-case))
	 (type-definition
	  (cdr (assoc (db3-type col) *db3-pgsql-type-mapping* :test #'string=))))
    (format nil "~a ~22t ~a" column-name type-definition)))

(defun list-all-columns (db3-file-name
			 &optional (table-name (pathname-name db3-file-name)))
  "Return the list of columns for the given DB3-FILE-NAME."
  (with-open-file (stream db3-file-name
			  :direction :input
                          :element-type '(unsigned-byte 8))
    (let ((db3 (make-instance 'db3:db3)))
      (db3:load-header db3 stream)
      (cons table-name
	    (loop
	       for field in (db3::fields db3)
	       collect (make-db3-field (db3::field-name field)
				       (db3::field-type field)
				       (db3::field-length field)))))))

(declaim (inline logical-to-boolean
		 db3-trim-string
		 db3-date-to-pgsql-date))

(defun logical-to-boolean (value)
  "Convert a DB3 logical value to a PostgreSQL boolean."
  (if (string= value "?") nil value))

(defun db3-trim-string (value)
  "DB3 Strings a right padded with spaces, fix that."
  (string-right-trim '(#\Space) value))

(defun db3-date-to-pgsql-date (value)
  "Convert a DB3 date to a PostgreSQL date."
  (let ((year  (subseq value 0 4))
	(month (subseq value 4 6))
	(day   (subseq value 6 8)))
    (format nil "~a-~a-~a" year month day)))

(defun transforms (input)
  "Return the list of transforms to apply to each row of data in order to
   convert values to PostgreSQL format"
  (with-open-file (stream input
			  :direction :input
                          :element-type '(unsigned-byte 8))
    (let ((db3 (make-instance 'db3:db3)))
      (db3:load-header db3 stream)
      (loop
	 for field in (db3::fields db3)
	 for type = (db3::field-type field)
	 collect
	   (cond ((string= type "L") #'logical-to-boolean)
		 ((string= type "C") #'db3-trim-string)
		 ((string= type "D") #'db3-date-to-pgsql-date)
		 (t                  nil))))))


;;;
;;; Integration with pgloader
;;;
(defun map-rows (filename &key process-row-fn)
  "Extract DB3 data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (with-open-file (stream filename
			  :direction :input
                          :element-type '(unsigned-byte 8))
    (let ((db3 (make-instance 'db3:db3)))
      (db3:load-header db3 stream)
      (loop
	 with count = (db3:record-count db3)
	 repeat count
	 for row-array = (db3:load-record db3 stream)
	 do (funcall process-row-fn (coerce row-array 'list))
	 finally (return count)))))

(defun copy-to (db3-filename pgsql-copy-filename)
  "Extract data from DB3 file into a PotgreSQL COPY TEXT formated file"
  (with-open-file (text-file pgsql-copy-filename
			     :direction :output
			     :if-exists :supersede
			     :external-format :utf-8)
    (let ((transforms (transforms db3-filename)))
      (map-rows db3-filename
		:process-row-fn
		(lambda (row)
		  (pgloader.pgsql:format-row text-file
					     row
					     :transforms transforms))))))

;;;
;;; Export MySQL data to our lparallel data queue. All the work is done in
;;; other basic layers, simple enough function.
;;;
(defun copy-to-queue (filename dataq table-name)
  "Copy data from DB3 file FILENAME into queue DATAQ"
  (let ((read (pgloader.queue:map-push-queue dataq #'map-rows filename)))
    (pgstate-incf *state* table-name :read read)))

(defun stream-file (filename
		    &key
		      dbname
		      state-before
		      (table-name (pathname-name filename))
		      create-table
		      truncate)
  "Open the DB3 and stream its content to a PostgreSQL database."
  (let* ((summary     (null *state*))
	 (*state*     (or *state* (make-pgstate))))

    (with-stats-collection (dbname "create, truncate"
				   :state state-before
				   :summary summary)
      (with-pgsql-transaction (dbname)
	(when create-table
	  (log-message :notice "Create table \"~a\"" table-name)
	  (log-message :info "~a" create-table-sql)
	  (create-tables (list-all-columns filename table-name)))

	(when (and truncate (not create-table))
	  ;; we don't TRUNCATE a table we just CREATEd
	  (let ((truncate-sql  (format nil "TRUNCATE ~a;" table-name)))
	    (log-message :notice "~a" truncate-sql)
	    (pgsql-execute truncate-sql)))))

    (let* ((lp:*kernel* (make-kernel 2))
	   (channel     (lp:make-channel))
	   (dataq       (lq:make-queue :fixed-capacity 4096)))

      (with-stats-collection (dbname table-name :state *state* :summary summary)
	(log-message :notice "COPY \"~a\" from '~a'" table-name filename)
	(lp:submit-task channel #'copy-to-queue filename dataq table-name)

	;; and start another task to push that data from the queue to PostgreSQL
	(lp:submit-task channel
			#'pgloader.pgsql:copy-from-queue
			dbname table-name dataq
			:truncate truncate
			:transforms (transforms filename))

	;; now wait until both the tasks are over, and kill the kernel
	(loop for tasks below 2 do (lp:receive-result channel)
	   finally
	     (log-message :info "COPY \"~a\" done." table-name)
	     (lp:end-kernel))))))

;;;
;;; Tools to handle PostgreSQL data format
;;;
(in-package :pgloader.pgsql)

;;;
;;; Format row to PostgreSQL COPY format, the TEXT variant.
;;;
(defun format-row (stream row &key transforms)
  "Add a ROW in the STREAM, formating ROW in PostgreSQL COPY TEXT format.

See http://www.postgresql.org/docs/9.2/static/sql-copy.html#AEN66609 for
details about the format, and format specs."
  (let* (*print-circle* *print-pretty*)
    (loop
       for (col . more?) on row
       for fn in transforms
       for preprocessed-col = (apply-transform-function fn col)
       ;; still accept postmodern :NULL in "preprocessed" data
       do (if (or (null preprocessed-col)
		  (eq :NULL preprocessed-col))
	      (format stream "~a~:[~;~c~]" "\\N" more? #\Tab)
	      (progn
		;; From PostgreSQL docs:
		;;
		;; In particular, the following characters must be preceded
		;; by a backslash if they appear as part of a column value:
		;; backslash itself, newline, carriage return, and the
		;; current delimiter character.
		(loop
		   for char across preprocessed-col
		   do (case char
			(#\\         (format stream "\\\\")) ; 2 chars here
			(#\Space     (princ #\Space stream))
			(#\Newline   (format stream "\\n")) ; 2 chars here
			(#\Return    (format stream "\\r")) ; 2 chars here
			(#\Tab       (format stream "\\t")) ; 2 chars here
			(#\Backspace (format stream "\\b")) ; 2 chars here
			(#\Page      (format stream "\\f")) ; 2 chars here
			(t           (format stream "~c" char))))
		(format stream "~:[~;~c~]" more? #\Tab))))
    (format stream "~%")))

;;;
;;; Read a file format in PostgreSQL COPY TEXT format, and call given
;;; function on each line.
;;;
(defun map-rows (filename &key process-row-fn)
  "Load data from a text file in PostgreSQL COPY TEXT format.

Each row is pre-processed then PROCESS-ROW-FN is called with the row as a
list as its only parameter.

Finally returns how many rows where read and processed."
  (with-open-file
      ;; we just ignore files that don't exist
      (input filename
	     :direction :input
	     :if-does-not-exist nil)
    (when input
      ;; read in the text file, split it into columns, process NULL columns
      ;; the way postmodern expects them, and call PROCESS-ROW-FN on them
      (loop
	 for line = (read-line input nil)
	 for row = (mapcar (lambda (x)
			     ;; we want Postmodern compliant NULLs
			     (if (string= "\\N" x) :null x))
			   ;; splitting is easy, it's always on #\Tab
			   ;; see format-row-for-copy for details
			   (sq:split-sequence #\Tab line))
	 while line
	 counting line into count
	 do (funcall process-row-fn row)
	 finally (return count)))))

;;;
;;; Read a file in PostgreSQL COPY TEXT format and load it into a PostgreSQL
;;; table using the COPY protocol. We expect PostgreSQL compatible data in
;;; that data format, so we don't handle any reformating here.
;;;
(defun copy-to-queue (table-name filename dataq &optional (*state* *state*))
  "Copy data from file FILENAME into lparallel.queue DATAQ"
  (let ((read
	 (pgloader.queue:map-push-queue dataq #'map-rows filename)))
    (pgstate-incf *state* table-name :read read)))

(defun copy-from-file (dbname table-name filename
		       &key
			 (truncate t)
			 (report nil))
  "Load data from clean COPY TEXT file to PostgreSQL, return how many rows."
  (let* ((*state*     (if report (pgloader.utils:make-pgstate) *state*))
	 (lp:*kernel*
	  (lp:make-kernel 2 :bindings
			  `((*pgconn-host* . ,*pgconn-host*)
			    (*pgconn-port* . ,*pgconn-port*)
			    (*pgconn-user* . ,*pgconn-user*)
			    (*pgconn-pass* . ,*pgconn-pass*)
			    (*pg-settings* . ',*pg-settings*)
			    (*state*       . ,*state*))))
	 (channel     (lp:make-channel))
	 (dataq       (lq:make-queue :fixed-capacity 4096)))

    (log-message :debug "pgsql:copy-from-file: ~a ~a ~a" dbname table-name filename)

    (when report
      (pgstate-add-table *state* dbname table-name))

    (lp:submit-task channel #'copy-to-queue table-name filename dataq *state*)

    ;; and start another task to push that data from the queue to PostgreSQL
    (lp:submit-task channel
		    #'pgloader.pgsql:copy-from-queue
		    dbname table-name dataq
		    :state *state*
		    :truncate truncate)

    ;; now wait until both the tasks are over, and measure time it took'em
    (multiple-value-bind (res secs)
	(timing
	 (loop for tasks below 2 do (lp:receive-result channel)))
      (declare (ignore res))
      (when report (pgstate-incf *state* table-name :secs secs)))

    (when report
      (report-table-name table-name)
      (report-pgtable-stats *state* table-name))))


;;;
;;; Tools to handle PostgreSQL data format
;;;
(in-package :pgloader.pgsql)

;;;
;;; PostgreSQL Tools connecting to a database
;;;
(defun get-connection-spec (dbname &key (with-port t))
  "pomo:with-connection and cl-postgres:open-database and open-db-writer are
   not using the same connection spec format..."
  (let ((conspec (list dbname *pgconn-user* *pgconn-pass* *pgconn-host*)))
    (if with-port
      (append conspec (list :port *pgconn-port*))
      (append conspec (list *pgconn-port*)))))

(defun truncate-table (dbname table-name)
  "Truncate given TABLE-NAME in database DBNAME"
  (pomo:with-connection (get-connection-spec dbname)
    (pomo:execute (format nil "truncate ~a;" table-name))))

(defun list-databases (&optional (username "postgres"))
  "Connect to a local database and get the database list"
  (pomo:with-connection (let ((*pgconn-user* username))
			  (get-connection-spec "postgres"))
    (loop for (dbname) in (pomo:query
			   "select datname
                              from pg_database
                             where datname !~ 'postgres|template'")
       collect dbname)))

(defun list-tables (dbname)
  "Return an alist of tables names and list of columns to pay attention to."
  (pomo:with-connection
      (get-connection-spec dbname)

    (loop for (relname colarray) in (pomo:query "
select relname, array_agg(case when typname in ('date', 'timestamptz')
                               then attnum end
                          order by attnum)
      from pg_class c
           join pg_namespace n on n.oid = c.relnamespace
           left join pg_attribute a on c.oid = a.attrelid
           join pg_type t on t.oid = a.atttypid
     where c.relkind = 'r'
           and attnum > 0
           and n.nspname = 'public'
  group by relname
")
       collect (cons relname (loop
				for attnum across colarray
				unless (eq attnum :NULL)
				collect attnum)))))

(defun list-tables-cols (dbname)
  "Return an alist of tables names and number of columns."
  (pomo:with-connection
      (get-connection-spec dbname)

    (loop for (relname cols) in (pomo:query "
    select relname, count(attnum)
      from pg_class c
           join pg_namespace n on n.oid = c.relnamespace
           left join pg_attribute a on c.oid = a.attrelid
           join pg_type t on t.oid = a.atttypid
     where c.relkind = 'r'
           and attnum > 0
           and n.nspname = 'public'
  group by relname
")
       collect (cons relname cols))))

(defun reset-all-sequences (dbname)
  "Reset all sequences to the max value of the column they are attached to."
  (let ((connection
	 (apply #'cl-postgres:open-database
		(remove :port (get-connection-spec dbname)))))

    (cl-postgres:exec-query connection "listen seqs")

    (prog1
	(handler-case
	    (cl-postgres:exec-query connection "
DO $$
DECLARE
  n integer := 0;
  r record;
BEGIN
  FOR r in
       SELECT 'select '
               || trim(trailing ')'
                  from replace(pg_get_expr(d.adbin, d.adrelid),
                               'nextval', 'setval'))
               || ', (select max(' || a.attname || ') from only '
               || nspname || '.' || relname || '));' as sql
         FROM pg_class c
              JOIN pg_namespace n on n.oid = c.relnamespace
              JOIN pg_attribute a on a.attrelid = c.oid
              JOIN pg_attrdef d on d.adrelid = a.attrelid
                                 and d.adnum = a.attnum
                                 and a.atthasdef
        WHERE relkind = 'r' and a.attnum > 0
              and pg_get_expr(d.adbin, d.adrelid) ~ '^nextval'
  LOOP
    n := n + 1;
    EXECUTE r.sql;
  END LOOP;

  PERFORM pg_notify('seqs', n::text);
END;
$$; ")
	  ;; now get the notification signal
	  (cl-postgres:postgresql-notification (c)
	    (parse-integer (cl-postgres:postgresql-notification-payload c))))
    (cl-postgres:close-database connection))))

(defun execute (dbname sql)
  "Execute given SQL in DBNAME"
  (pomo:with-connection (get-connection-spec dbname)
    (pomo:execute sql)))

(defun set-session-gucs (alist)
  "Set given GUCs to given values for the current session."
  (loop
     for (name . value) in alist
     do (pomo:execute (format nil "SET ~a TO ~a" name value))))

;;;
;;; PostgreSQL formating tools
;;;
(defun apply-transform-function (fn col)
  "Apply the tranformation function FN to the value COL."
  (declare (inline))
  (if fn (funcall fn col) col))

(defun reformat-row (row &key transforms)
  "Reformat row as given by MySQL in a format compatible with cl-postgres"
  (loop
     for col in row
     for fn in transforms
     for transformed-col = (apply-transform-function fn col)
     ;; force nil values to being cl-postgres :null special value
     collect (if (null transformed-col) :null transformed-col)))

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
;;; Pop data from a lparallel.queue queue instance, reformat it assuming
;;; data in there are from cl-mysql, and copy it to a PostgreSQL table.
;;;
;;; First prepare the streaming function that batches the rows so that we
;;; are able to process them in case of errors.
;;;
(defvar *batch* nil "Current batch of rows being processed.")
(defvar *batch-size* 0 "How many rows are to be found in current *batch*.")

(defun make-copy-and-batch-fn (stream &key transforms)
  "Returns a function of one argument, ROW.

   When called, the function returned reformats the row, adds it into the
   PostgreSQL COPY STREAM, and push it to BATCH (a list). When batch's size
   is up to *copy-batch-size*, throw the 'next-batch tag with its current
   size."
  (lambda (row)
    (let ((reformated-row (reformat-row row :transforms transforms)))

      ;; maintain the current batch
      (push reformated-row *batch*)
      (incf *batch-size*)

      ;; send the reformated row in the PostgreSQL COPY stream
      (cl-postgres:db-write-row stream reformated-row)

      ;; return control in between batches
      (when (= *batch-size* *copy-batch-size*)
	(throw 'next-batch (cons :continue *batch-size*))))))

;;; The idea is to stream the queue content directly into the
;;; PostgreSQL COPY protocol stream, but COMMIT every
;;; *copy-batch-size* rows.
;;;
;;; That allows to have to recover from a buffer of data only rather
;;; than restart from scratch each time we have to find which row
;;; contains erroneous data. BATCH is that buffer.
(defun copy-from-queue (dbname table-name dataq
			&key
			  (truncate t)
			  ((:state *state*) *state*)
			  transforms)
  "Fetch data from the QUEUE until we see :end-of-data. Update *state*"
  (when truncate (truncate-table dbname table-name))

  (log-message :debug "pgsql:copy-from-queue: ~a ~a" dbname table-name)

  (let* ((conspec (get-connection-spec dbname :with-port nil)))
    (loop
       for retval =
	 (let* ((stream (cl-postgres:open-db-writer conspec table-name nil))
		(*batch* nil)
		(*batch-size* 0))
	   (log-message :debug "pgsql:copy-from-queue: starting new batch")
	   (set-session-gucs *pg-settings*)
	   (unwind-protect
		(let ((process-row-fn
		       (make-copy-and-batch-fn stream :transforms transforms)))
		  (catch 'next-batch
		    (pgloader.queue:map-pop-queue dataq process-row-fn)))
	     ;; in case of data-exception, split the batch and try again
	     (handler-case
		 (progn
		   (log-message :debug "pgsql:copy-from-queue: commit batch")
		   (cl-postgres:close-db-writer stream))
	       ((or
		 CL-POSTGRES-ERROR:UNIQUE-VIOLATION
		 CL-POSTGRES-ERROR:DATA-EXCEPTION) (e)
		 (progn
		   (log-message :debug "pgsql:copy-from-queue: ~a" e)
		   (retry-batch dbname table-name
				(nreverse *batch*) *batch-size*))))))

       ;; fetch how many rows we just pushed through, update stats
       for rows = (if (consp retval) (cdr retval) retval)
       for cont = (and (consp retval) (eq (car retval) :continue))
       do (pgstate-incf *state* table-name :rows rows)
       while cont)))

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
  (let* ((lp:*kernel* *loader-kernel*)
	 (channel     (lp:make-channel))
	 (dataq       (lq:make-queue :fixed-capacity 4096))
	 (*state*     (if report (pgloader.utils:make-pgstate) *state*)))

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

;;;
;;; When a batch has been refused by PostgreSQL with a data-exception, that
;;; means it contains non-conforming data. It could be only one row in the
;;; middle of the *copy-batch-size* rows.
;;;
;;; The general principle to filter out the bad row(s) is to split the batch
;;; in smaller ones, and try to COPY all of the smaller ones again,
;;; recursively. When the batch is containing only one row, we know that one
;;; is non conforming to PostgreSQL expectations (usually, data type input
;;; does not match, e.g. text is not proper utf-8).
;;;
;;; As we often need to split out a single bad row out of a full batch, we
;;; don't do the classical dichotomy but rather split the batch directly in
;;; lots of smaller ones.
;;;
;;;   split 1000 rows in 10 batches of 100 rows
;;;   split  352 rows in 3 batches of 100 rows + 1 batch of 52 rows
;;;
(defun process-bad-row (table-name condition row)
  "Add the row to the reject file, in PostgreSQL COPY TEXT format"
  ;; first, update the stats.
  (pgstate-incf *state* table-name :errs 1 :rows -1)

  ;; now, the bad row processing
  (let* ((table (pgstate-get-table *state* table-name))
	 (data  (pgtable-reject-data table))
	 (logs  (pgtable-reject-logs table)))

    ;; first log the rejected data
    (with-open-file (reject-data-file data
				      :direction :output
				      :if-exists :append
				      :if-does-not-exist :create
				      :external-format :utf-8)
      ;; the row has already been processed when we get here
      (format-row reject-data-file row))

    ;; now log the condition signaled to reject the data
    (with-open-file (reject-logs-file logs
				      :direction :output
				      :if-exists :append
				      :if-does-not-exist :create
				      :external-format :utf-8)
      ;; the row has already been processed when we get here
      (format reject-logs-file "~a~%" condition))))

;;;
;;; Compute the next batch size, must be smaller than the previous one or
;;; just one row to ensure the retry-batch recursion is not infinite.
;;;
(defun smaller-batch-size (batch-size processed-rows)
  "How many rows should we process in next iteration?"
  (let ((remaining-rows (- batch-size processed-rows)))

    (if (< remaining-rows *copy-batch-split*)
	1
	(min remaining-rows
	     (floor (/ batch-size *copy-batch-split*))))))

;;;
;;; The recursive retry batch function.
;;;
(defun retry-batch (dbname table-name batch batch-size)
  "Batch is a list of rows containing at least one bad row. Find it."
  (let* ((conspec (get-connection-spec dbname :with-port nil))
	 (current-batch-pos batch)
	 (processed-rows 0))
    (loop
       while (< processed-rows batch-size)
       do
	 (log-message :debug "pgsql:retry-batch: splitting current batch")
	 (let* ((current-batch current-batch-pos)
		(current-batch-size (smaller-batch-size batch-size
							processed-rows))
		(stream
		 (cl-postgres:open-db-writer conspec table-name nil)))

	   (log-message :debug "pgsql:retry-batch: current-batch-size = ~d"
			current-batch-size)

	   (unwind-protect
		(dotimes (i current-batch-size)
		  ;; rows in that batch have already been processed
		  (cl-postgres:db-write-row stream (car current-batch-pos))
		  (setf current-batch-pos (cdr current-batch-pos))
		  (incf processed-rows))

	     (handler-case
		 (cl-postgres:close-db-writer stream)

	       ;; the batch didn't make it, recurse
	       ((or
		 CL-POSTGRES-ERROR:UNIQUE-VIOLATION
		 CL-POSTGRES-ERROR:DATA-EXCEPTION) (condition)
		 ;; process bad data
		 (if (= 1 current-batch-size)
		     (process-bad-row table-name condition (car current-batch))
		     ;; more than one line of bad data: recurse
		     (retry-batch dbname table-name
				  current-batch current-batch-size)))))))))

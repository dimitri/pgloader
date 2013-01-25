(in-package :galaxya-loader)

;;;
;;; Reformating Tools, because MySQL has not the same idea about its data
;;; than PostgreSQL has. Ever heard of year 0000? MySQL did...
;;;
(defun mysql-fix-date (datestr)
  (cond
    ((null datestr) nil)
    ((string= datestr "") nil)
    ((string= datestr "0000-00-00") nil)
    ((string= datestr "0000-00-00 00:00:00") nil)
    (t datestr)))

(defun pgsql-reformat-null-value (value)
  "cl-mysql returns nil for NULL and cl-postgres wants :NULL"
  (if (null value) :NULL value))

(defun pgsql-reformat-row (row &key date-columns)
  "Reformat row as given by MySQL in a format compatible with cl-postgres"
  (loop
     for i from 1
     for col in row
     for no-zero-date-col = (if (member i date-columns)
				(mysql-fix-date col)
				col)
     collect (pgsql-reformat-null-value no-zero-date-col)))

;;;
;;; Implement PostgreSQL COPY format, the TEXT variant.
;;;
(defun pgsql-text-copy-format (stream row &key date-columns)
  "Add a csv row in the stream"
  (let* (*print-circle* *print-pretty*)
    (loop
       for i from 1
       for (col . more?) on row
       for preprocessed-col = (if (member i date-columns)
				  (mysql-fix-date col)
				  col)
       do (if (null preprocessed-col)
	      (format stream "~a~:[~;~c~]" "\\N" more? #\Tab)
	      (progn
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
;;; Map a function to each row extracted from MySQL
;;;
(defun mysql-map-rows (dbname table-name process-row-fn
		       &key
			 (host *myconn-host*)
			 (user *myconn-user*)
			 (pass *myconn-pass*))
  "Extract MySQL data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (cl-mysql:connect :host host :user user :password pass)

  (unwind-protect
       (progn
	 ;; Ensure we're talking utf-8 and connect to DBNAME in MySQL
	 (cl-mysql:query "SET NAMES 'utf8'")
	 (cl-mysql:query "SET character_set_results = utf8;")
	 (cl-mysql:use dbname)

	 (let* ((sql (format nil "SELECT * FROM ~a;" table-name))
		(q   (cl-mysql:query sql :store nil))
		(rs  (cl-mysql:next-result-set q)))
	   (declare (ignore rs))

	   ;; Now fetch MySQL rows directly in the stream
	   (loop
	      for row = (cl-mysql:next-row q :type-map (make-hash-table))
	      while row
	      counting row into count
	      do (funcall process-row-fn row)
	      finally (return count))))

    ;; free resources
    (cl-mysql:disconnect)))

;;;
;;; Use mysql-map-rows and pgsql-text-copy-format to fill in a CSV file on
;;; disk with MySQL data in there.
;;;
(defun mysql-copy-text-format (dbname table-name filename
			       &key
			       date-columns
			       (host *myconn-host*)
			       (user *myconn-user*)
			       (pass *myconn-pass*))
  "Extrat data from MySQL in PostgreSQL COPY TEXT format"
  (with-open-file (text-file filename
			     :direction :output
			     :if-exists :supersede
			     :external-format :utf8)
    (mysql-map-rows dbname table-name
		    (lambda (row)
		      (pgsql-text-copy-format
		       text-file
		       row
		       :date-columns date-columns))
		    :host host
		    :user user
		    :pass pass)))

;;;
;;; Some MySQL other tools
;;;
(defun list-tables-in-mysql-db (dbname
				&key
				(host *myconn-host*)
				(user *myconn-user*)
				(pass *myconn-pass*))
  "As the name says"
  (cl-mysql:connect :host host :user user :password pass)

  (unwind-protect
       (progn
	 (cl-mysql:use dbname)
	 ;; that returns a pretty weird format, process it
	 (mapcan #'identity (caar (cl-mysql:list-tables))))
    ;; free resources
    (cl-mysql:disconnect)))


(defun mysql-export-all-tables (dbname
				&key
				only-tables
				(host *myconn-host*)
				(user *myconn-user*)
				(pass *myconn-pass*))
  "Export MySQL tables into as many TEXT files, in the PostgreSQL COPY format"
  (let ((pgsql-date-columns-alist (get-table-list dbname))
	(total-count 0)
	(total-seconds 0))
    (format t "~&~30@a  ~9@a  ~9@a" "table name" "rows" "time")
    (format t "~&------------------------------  ---------  ---------")
    (loop
       for table-name in (list-tables-in-mysql-db dbname
						  :host host
						  :user user
						  :pass pass)
       for filename = (get-csv-pathname dbname table-name)
       when (or (null only-tables)
		(member table-name only-tables :test #'equal))
       do
	 (format t "~&~30@a  " table-name)
	 (multiple-value-bind (result seconds)
	     (timing
	      (mysql-copy-text-format
	       dbname
	       table-name
	       (get-csv-pathname dbname table-name)
	       :date-columns (cdr (assoc table-name pgsql-date-columns-alist))))
	   (when result
	     (incf total-count result)
	     (incf total-seconds seconds)
	     (format t "~9@a  ~9@a"
		     result (format-interval seconds nil))))
       finally
	 (format t "~&------------------------------  ---------  ---------")
	 (format t "~&~30@a  ~9@a  ~9@a" "Total export time"
		 total-count (format-interval total-seconds nil))
	 (return (values total-count (float total-seconds))))))

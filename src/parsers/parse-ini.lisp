;;;
;;; Compatibility package to read old configuration file format.
;;;

(in-package :pgloader.ini)

(defparameter *global-section* "pgsql")

(defstruct params
  filename table format is-template use-template
  fields columns
  truncate encoding logs rejects gucs
  separator null-as empty-string skip-lines)

(defun process-true-false (value)
  "parse python boolean values"
  (cond ((string-equal "True" value)  t)
	((string-equal "False" value) nil)
	(t value)))

(defun read-default-value-for-param (config option &optional default)
  "Fetch value for OPTION in the global section."
  (if (ini:has-option-p config *global-section* option)
      (ini:get-option config *global-section* option)
      default))

(defun read-value-for-param (config section option &key template default)
  "Read the value of OPTION in the SECTION part of the CONFIG or its
   TEMPLATE when one is defined, finally using provided DEFAULT."
  (cond ((ini:has-option-p config section option)
	 (ini:get-option config section option))

	(template
         ;; don't inherit the is-template property from the template
         (unless (string-equal "template" option)
           (if (ini:has-option-p config template option)
               (ini:get-option config template option)
               (read-default-value-for-param config option default))))

	(t (read-default-value-for-param config option default))))

(defmethod set-param ((params params) config section option param
		      &optional default)
  "Set the params structure slot PARAM, reading its value in the SECTION
   part of the CONFIG or its TEMPLATE when one is defined, finally using
   provided DEFAULT."
  (let ((value
	 (process-true-false
	  (read-value-for-param config section option
				:template (params-use-template params)
				:default default))))
    (setf (slot-value params param) value)))

(defmethod set-gucs ((params params) config section)
  (let* ((template (params-use-template params))
	 (encoding
	  (string-trim "'"
		       (read-value-for-param config section "client_encoding"
					     :template template)))
	 (datestyle  (read-value-for-param config section "datestyle"
					   :template template)))
    (setf (params-gucs params)
	  (append
	   (when encoding  (list (cons "client_encoding" encoding)))
	   (when datestyle (list (cons "datestyle" datestyle)))
	   (merge-gucs
	    (get-gucs config section)
	    (when template (get-gucs config template))
	    (get-gucs config *global-section*))))))

(defun get-gucs (config section)
  "Get PostgreSQL settings from SECTION."
  (loop
     for (option . value) in (ini:items config section)
     when (and (< 10 (length option)) (string= "pg_option_" option :end2 10))
     collect (cons (subseq option 10) value)))

(defun merge-gucs (&rest gucs)
  "Merge several guc lists into a consolidated one. When the same GUC is
   found more than once, we keep the one found first."
  (remove-duplicates (apply #'append gucs)
		     :from-end t
		     :key #'car
		     :test #'string=))

(defun user-defined-columns (config section)
  "Fetch all option that begin with udc_ as user defined columns"
  (loop for (option . value) in (ini:items config section)
     when (and (< 4 (length option)) (string= "udc_" option :end2 4))
     collect (cons (subseq option 4) value)))

(defun split-columns-specs (colspecs)
  "Return an alist of column name and column position from given COLSPEC"
  (loop
     for count from 1
     for raw in (sq:split-sequence #\, colspecs)
     for colspec = (string-trim " " raw)
     for (name pos) = (sq:split-sequence #\: colspec)
     collect (cons name (or (when pos (parse-integer pos)) count))))

(defun get-pgsql-column-specs (config section)
  "Connect to PostgreSQL to get the column specs."
  (destructuring-bind (&key host port user pass dbname table-name)
      (get-connection-params config section)
    (let ((pgconn (make-instance 'pgsql-connection
                                 :host host
                                 :port port
                                 :user user
                                 :pass pass
                                 :name dbname
                                 :table-name table-name)))
      (loop
         :for pos :from 1
         :for name :in (list-columns pgconn table-name)
         :collect (cons name pos)))))

(defun parse-columns-spec (string config section &key trailing-sep)
  "Parse old-style columns specification, such as:
    *                             -->  nil
    x, y, a, b, d:6, c:5          -->  \"x, y, a, b, d, c\"

   Returns the list of fields to read from the file and the list of columns
   to fill-in in the database as separate values."
  (let* ((colspecs
	  (if (string= string "*")
	      (get-pgsql-column-specs config section)
	      (split-columns-specs string))))
    (values (append
	     (mapcar #'car (sort (copy-list colspecs) #'< :key #'cdr))
	     (when trailing-sep '("trailing")))
	    (mapcar #'car colspecs))))

(defun parse-only-cols (columns only-cols)
  "  columns          = x, y, a, b, d:6, c:5
     only_cols        = 3-6

     Note that parsing the columns value has already been done for us, what
     we are given here actually is (x y a b d c)

     Returns (a b d c)"
  (let ((indices
	 (loop
	    for raw in (sq:split-sequence #\, only-cols)
	    for range = (string-trim " " raw)
	    for (lower upper) = (mapcar #'parse-integer
					(sq:split-sequence #\- range))
	    when upper append (loop for i from lower to upper collect i)
	    else collect lower)))
   (loop
      with cols = (coerce columns 'vector)
      for i in indices
      collect (aref cols (- i 1)))))

(defun compute-columns (columns only-cols copy-columns user-defined
			config section)
  "For columns, if only-cols is set, restrict to that. If copy-columns is
   set, use that and replace references to user defined columns."
  (cond (only-cols
	 ;; that's again something kind of special
	 (parse-only-cols columns only-cols))

	(copy-columns
	 ;; that's the format used when user-defined columns are in play
	 (multiple-value-bind (fields columns)
	     (parse-columns-spec copy-columns config section)
	   (declare (ignore fields))
	   (mapcar
	    (lambda (colname)
	      (let ((constant
		     (cdr (assoc colname user-defined :test #'string=))))
		(if constant
		    (format nil "~a ~a using ~s" colname "text" constant)
		    colname)))
	    columns)))

	(t
	 columns)))

(defun parse-section (config section &optional (params (make-params)))
  "Parse a configuration section into a params structure."
  (unless (params-is-template params)
    (loop for (option . param) in '(("use_template"    . use-template)
				    ("template"        . is-template)
				    ("reject_log"      . logs)
				    ("reject_data"     . rejects)
				    ("table"           . table)
				    ("format"          . format)
				    ("filename"        . filename)
				    ("truncate"        . truncate)
				    ("input_encoding"  . encoding)
				    ("reject_log"      . logs)
				    ("reject_data"     . rejects)
				    ("field_sep"       . separator)
				    ("null"            . null-as)
				    ("empty_string"    . empty-string)
				    ("skip_head_lines" . skip-lines))
       do (set-param params config section option param))

    ;; now parse gucs
    (set-gucs params config section)

    ;; now parse fields and columns
    (let* ((template       (params-use-template params))
	   (trailing-sep   (read-value-for-param config section "trailing_sep"
						 :template template))
	   (columns        (read-value-for-param config section "columns"
						 :template template))
	   (user-defined   (append
			    (user-defined-columns config section)
			    (when template
			      (user-defined-columns config template))
			    (user-defined-columns config *global-section*)))
	   (copy-columns   (read-value-for-param config section "copy_columns"
						 :template template))
	   (only-cols      (read-value-for-param config section "only_cols"
						 :template template)))

      ;; make sense of the old cruft
      (multiple-value-bind (fields columns)
	  (parse-columns-spec columns config section :trailing-sep trailing-sep)
	(setf (params-fields params)  fields)
	(setf (params-columns params) (compute-columns columns
						       only-cols
						       copy-columns
						       user-defined
						       config
						       section))))
    params))

(defun get-connection-params (config section)
  "Return a property list with connection parameters for SECTION."
  (let ((defaults (pgloader:parse-target-string "pgsql:///")))
    (append
     (loop
        for (param option section default)
        in `((:host   "host"   ,*global-section* ,(db-host defaults))
             (:port   "port"   ,*global-section* ,(prin1-to-string
                                                   (db-port defaults)))
             (:user   "user"   ,*global-section* ,(db-user defaults))
             (:pass   "pass"   ,*global-section* ,(db-pass defaults))
             (:dbname "base"   ,*global-section* nil))
        append
          (list param
                (coerce
                 (read-value-for-param config section option :default default)
                 'simple-string)))
     ;; fetch table name from current section or its template maybe
     (let ((template (read-value-for-param config section "use_template")))
       (list :table-name
             (read-value-for-param config section "table" :template template))))))

(defun get-connection-string (config section)
  "Return the connection parameters as a postgresql:// string."
  (destructuring-bind (&key host port user pass dbname table-name)
      (get-connection-params config section)
    (format nil "postgresql://~a:~a@~a:~a/~a?~a"
	    user pass host port dbname table-name)))

(defun read-ini-file (filename)
  (let ((config (ini:make-config)))
    (ini:read-files config (list filename))))

(defun parse-ini-file (filename)
  "Parse an old-style INI file into a list of PARAMS structures"
  (let* ((config   (read-ini-file filename))
	 (sections
	  (remove-if
	   (lambda (s) (member s '("default" *global-section*) :test #'string=))
	   (ini:sections config))))

    (remove-if #'null (mapcar (lambda (s) (parse-section config s)) sections))))

(defun print-csv-option (params option)
  "Print a CSV option in the new format."
  (let ((value (when (slot-exists-p params option)
		 (slot-value params option))))
    (case option
      (truncate      (when value "truncate"))
      (quote         (format nil "fields optionally enclosed by '~c'" #\"))
      (escape        (format nil "fields escaped by double-quote"))
      (separator     (format nil "fields terminated by '~c'" (aref value 0)))
      (skip-lines    (when value
		       (format nil "skip header = ~a" value))))))

(defun write-command-to-string (config section
				&key with-data-inline (end-command t))
  "Return the new syntax for the command found in SECTION.

   When WITH-DATA-INLINE is true, instead of using the SECTION's filename
   option, use the constant INLINE in the command."
  (let ((params (parse-section config section)))
    (when (and (params-filename params)
	       (params-separator params))
      (with-output-to-string (s)
	(format s "LOAD CSV~%")

	(format s "     FROM ~a ~@[WITH ENCODING ~a~]~%"
		(if with-data-inline "inline"
		    (format nil "'~a'" (params-filename params)))
		(when (params-encoding params)
		  (string-trim "'" (params-encoding params))))
	(format s "        ~@[(~{~&~10T~a~^,~}~%~8T)~]~%" (params-fields params))

	(format s "     INTO ~a~%" (get-connection-string config section))
	(format s "        ~@[(~{~&~10T~a~^,~}~%~8T)~]~%" (params-columns params))

	;; CSV options
	(format s "~%     WITH ~{~a~^,~%~10T~}~%"
		(loop for name in '(truncate skip-lines quote escape separator)
		   for option = (print-csv-option params name)
		   when option collect it))

	;; GUCs
	(format s "~%      SET ~{~a~^,~&~10T~}"
		(loop for (name . setting) in (params-gucs params)
		   collect (format nil "~a to '~a'" name setting)))

	;; End the command with a semicolon, unless asked not to
	(format s "~@[;~]" end-command)))))

(defun convert-ini-into-commands (filename)
  "Read the INI file at FILENAME and convert each section of it to a command
   in the new pgloader format."
  (let ((config (read-ini-file filename)))
    (format t "~{~a~^~%~%~%~}"
	    (loop for section in (ini:sections config)
	       for command = (write-command-to-string config section)
	       when command collect it))))

(defun convert-ini-into-files (filename target-directory
			       &key
				 with-data-inline
				 include-sql-file)
  "Reads the INI file at FILENAME and creates files names <section>.load for
   each section in the INI file, in TARGET-DIRECTORY.

   When WITH-DATA-INLINE is true, read the CSV file listed as the section's
   filename and insert its content in the command itself, as inline data.

   When INCLUDE-SQL-FILE is :if-exists, try to find a sibling file to the
   data file, with the same name and with the \"sql\" type, and use its
   content in a BEFORE LOAD DO clause.

   When INCLUDE-SQL-FILE is t, not finding the SQL file is an error."
  (let ((config (read-ini-file filename)))

    ;; first mkdir -p
    (ensure-directories-exist target-directory)

    (loop
       for section in (ini:sections config)
       for target = (make-pathname :directory target-directory
				   :name section
				   :type "load")
       for command = (write-command-to-string config section
					      :with-data-inline with-data-inline
					      :end-command nil)
       when command
       do (with-open-file (c target
			     :direction :output
			     :if-exists :supersede
			     :if-does-not-exist :create
			     :external-format :utf-8)
	    (format c "~a" command)

	    (let* ((params   (parse-section config section))
		   (datafile
		    (merge-pathnames (params-filename params)
				     (directory-namestring filename)))
		   (sqlfile
		    (make-pathname :directory (directory-namestring datafile)
				   :name (pathname-name datafile)
				   :type "sql"))
		   (sql-file-exists (probe-file sqlfile))
		   (sql-commands    (when sql-file-exists
                                      (read-file-into-string sqlfile))))
	      ;; First
	      (if include-sql-file
		  (if sql-file-exists
		      (progn
			(format c "~%~%   BEFORE LOAD DO")
			(format c "~{~&~3T$$ ~a; $$~^,~};~%"
				(remove-if
				 (lambda (x)
				   (string= ""
					    (string-trim '(#\Space
							   #\Return
							   #\Linefeed) x)))
				 (sq:split-sequence #\; sql-commands))))
		      (unless (eq sql-file-exists :if-exists)
			(error "File not found: ~s" sqlfile)))
		  ;; don't include sql file
		  (format c ";~%"))

	      (when with-data-inline
		(let* ((params   (parse-section config section))
		       (datafile
			(merge-pathnames (params-filename params)
					 (directory-namestring filename))))
		  (format c "~%~%~%~%~a"
                          (read-file-into-string datafile))))))
       and collect target)))

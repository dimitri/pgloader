;;;
;;; Now the main command, one of
;;;
;;;  - LOAD FROM some files
;;;  - LOAD DATABASE FROM a MySQL remote database
;;;  - LOAD MESSAGES FROM a syslog daemon receiver we're going to start here
;;;

(in-package #:pgloader.parser)

(defrule end-of-command (and ignore-whitespace #\; ignore-whitespace)
  (:constant :eoc))

(defrule command (and (or load-archive
			  load-csv-file
			  load-fixed-cols-file
			  load-dbf-file
                          load-ixf-file
			  load-mysql-database
                          load-mssql-database
			  load-sqlite-database
			  ;; load-syslog-messages
                          )
		      end-of-command)
  (:lambda (cmd)
    (bind (((command _) cmd)) command)))

(defrule commands (+ command))

(defun parse-commands (commands)
  "Parse a command and return a LAMBDA form that takes no parameter."
  (parse 'commands commands))

(defun inject-inline-data-position (command position)
  "We have '(:inline nil) somewhere in command, have '(:inline position) instead."
  (loop
     for s-exp in command
     when (equal '(:inline nil) s-exp) collect (list :inline position)
     else collect (if (and (consp s-exp) (listp (cdr s-exp)))
		      (inject-inline-data-position s-exp position)
		      s-exp)))

(defun process-relative-pathnames (filename command)
  "Walk the COMMAND to replace relative pathname with absolute ones, merging
   them within the directory where we found the command FILENAME."
  (loop
     for s-exp in command
     when (pathnamep s-exp)
     collect (if (uiop:relative-pathname-p s-exp)
		 (uiop:merge-pathnames* s-exp filename)
		 s-exp)
     else
     collect (if (and (consp s-exp) (listp (cdr s-exp)))
		 (process-relative-pathnames filename s-exp)
		 s-exp)))

(defun parse-commands-from-file (maybe-relative-filename
                                 &aux (filename
                                       ;; we want a truename here
                                       (probe-file maybe-relative-filename)))
  "The command could be using from :inline, in which case we want to parse
   as much as possible then use the command against an already opened stream
   where we moved at the beginning of the data."
  (if filename
      (log-message :log "Parsing commands from file ~s~%" filename)
      (error "Can not find file: ~s" maybe-relative-filename))

  (process-relative-pathnames
   filename
   (let ((*cwd* (make-pathname :defaults filename :name nil :type nil))
         (*data-expected-inline* nil)
	 (content (read-file-into-string filename)))
     (multiple-value-bind (commands end-commands-position)
	 (parse 'commands content :junk-allowed t)

       ;; INLINE is only allowed where we have a single command in the file
       (if *data-expected-inline*
	   (progn
	     (when (= 0 end-commands-position)
	       ;; didn't find any command, leave error reporting to esrap
	       (parse 'commands content))

	     (when (and *data-expected-inline*
			(null end-commands-position))
	       (error "Inline data not found in '~a'." filename))

	     (when (and *data-expected-inline* (not (= 1 (length commands))))
	       (error (concatenate 'string
				   "Too many commands found in '~a'.~%"
				   "To use inline data, use a single command.")
		      filename))

	     ;; now we should have a single command and inline data after that
	     ;; replace the (:inline nil) found in the first (and only) command
	     ;; with a (:inline position) instead
	     (list
	      (inject-inline-data-position
	       (first commands) (cons filename end-commands-position))))

	   ;; There was no INLINE magic found in the file, reparse it so that
	   ;; normal error processing happen
	   (parse 'commands content))))))

(defun run-commands (source
		     &key
		       (start-logger t)
                       ((:summary *summary-pathname*) *summary-pathname*)
		       ((:log-filename *log-filename*) *log-filename*)
		       ((:log-min-messages *log-min-messages*) *log-min-messages*)
		       ((:client-min-messages *client-min-messages*) *client-min-messages*))
  "SOURCE can be a function, which is run, a list, which is compiled as CL
   code then run, a pathname containing one or more commands that are parsed
   then run, or a commands string that is then parsed and each command run."

  (with-monitor (:start-logger start-logger)
    (let* ((funcs
            (typecase source
              (function (list source))

              (list     (list (compile nil source)))

              (pathname (mapcar (lambda (expr) (compile nil expr))
                                (parse-commands-from-file source)))

              (t        (mapcar (lambda (expr) (compile nil expr))
                                (if (probe-file source)
                                    (parse-commands-from-file source)
                                    (parse-commands source)))))))

      ;; maybe duplicate the summary to a file
      (let* ((summary-stream (when *summary-pathname*
                               (open *summary-pathname*
                                     :direction :output
                                     :if-exists :rename
                                     :if-does-not-exist :create)))
             (*report-stream* (or summary-stream *standard-output*)))
        (unwind-protect
             ;; run the commands
             (loop for func in funcs do (funcall func))

          ;; cleanup
          (when summary-stream (close summary-stream)))))))


;;;
;;; Parse an URI without knowing before hand what kind of uri it is.
;;;
(defvar *db-uri-prefix-list* '((:postgresql . ("pgsql://"
                                               "postgres://"
                                               "postgresql://"))
                               (:sqlite     . ("sqlite://"))
                               (:mysql      . ("mysql://"))
                               (:mssql      . ("mssql://"))))

(defun parse-db-uri-prefix (source-string)
  "See if SOURCE-STRING starts with a known dburi"
  (loop :for (type . prefixes) :in *db-uri-prefix-list*
     :for prefix := (loop :for prefix :in prefixes
                       :when (let ((plen (length prefix)))
                               (and (< plen (length source-string))
                                    (string-equal prefix source-string :end2 plen)))
                       :return prefix)
     :when prefix :return type))

(defvar *data-source-filename-extensions*
  '((:csv     . ("csv" "tsv" "txt" "text"))
    (:sqlite  . ("sqlite" "db"))
    (:dbf     . ("db3" "dbf"))
    (:ixf     . ("ixf"))))

(defun parse-filename-for-source-type (filename)
  "Given just an existing filename, decide what data source might be found
   inside..."
  (multiple-value-bind (abs paths filename no-path-p)
      (uiop:split-unix-namestring-directory-components
       (uiop:native-namestring filename))
    (declare (ignore abs paths no-path-p))
    (let ((dotted-parts (reverse (sq:split-sequence #\. filename))))
      (destructuring-bind (extension name-or-ext &rest parts)
          dotted-parts
        (declare (ignore parts))
        (if (string-equal "tar" name-or-ext) :archive
            (loop :for (type . extensions) :in *data-source-filename-extensions*
               :when (member extension extensions :test #'string-equal)
               :return type))))))

(defun parse-source-string-for-type (type source-string)
  "use the parse rules as per xxx-source rules"
  (let ((source (case type
                  (:csv        (parse 'csv-file-source      source-string))
                  (:fixed      (parse 'fixed-file-source    source-string))
                  (:dbf        (parse 'filename-or-http-uri source-string))
                  (:ixf        (parse 'filename-or-http-uri source-string))
                  (:sqlite     (parse 'sqlite-uri           source-string))
                  (:postgresql (parse 'pgsql-uri            source-string))
                  (:mysql      (parse 'mysql-uri            source-string))
                  (:mssql      (parse 'mssql-uri            source-string)))))
    (values type source)))

(defun parse-source-string (source-string)
  "Guess type from SOURCE-STRING then parse it accordingly."
  (cond ((probe-file (uiop:parse-unix-namestring source-string))
         (let ((type (parse-filename-for-source-type source-string)))
           (parse-source-string-for-type type source-string)))

        ((and source-string
              (< (length "http://") (length source-string))
              (string-equal "http://" source-string :end2 (length "http://")))
         (let ((type (parse-filename-for-source-type
                      (puri:uri-path (puri:parse-uri source-string)))))
           (case type
             (:csv    (log-message :fatal "No HTTP support for CSV files yet."))
             (:fixed  (log-message :fatal "No HTTP support for FIXED files yet."))
             (:sqlite (parse-source-string-for-type :sqlite source-string))
             (:db3    (parse-source-string-for-type :db3 source-string))
             (:ixf    (parse-source-string-for-type :ixf source-string)))))

        ((and source-string (parse-db-uri-prefix source-string))
         (let* ((type (parse-db-uri-prefix source-string)))
           (multiple-value-bind (type conn)
               (parse-source-string-for-type type source-string)
             (if (eq type (getf conn :type))
                 (values type conn)
                 (log-message :fatal "Parsed a ~s connection string for type ~s")))))

        (t nil)))

(defun parse-target-string (target-string)
  (parse 'pgsql-uri target-string))


;;;
;;; Command line accumulative options parser
;;;
(defun parse-cli-gucs (gucs)
  "Parse PostgreSQL GUCs as per the SET clause when we get them from the CLI."
  (loop :for guc :in gucs
     :collect (parse 'generic-option guc)))

(defrule dbf-type-name (or "dbf" "db3") (:constant "dbf"))

(defrule cli-type (or "csv"
                      "fixed"
                      dbf-type-name
                      "ixf"
                      "sqlite"
                      "mysql"
                      "mssql")
  (:text t))

(defun parse-cli-type (type)
  "Parse the --type option"
  (when type
   (intern (string-upcase (parse 'cli-type type)) (find-package "KEYWORD"))))

(defun parse-cli-encoding (encoding)
  "Parse the --encoding option"
  (if encoding
      (make-external-format (find-encoding-by-name-or-alias encoding))
      :utf-8))

(defun parse-cli-fields (type fields)
  "Parse the --fields option."
  (loop :for field :in fields
     :collect (parse (case type
                       (:csv   'csv-source-field)
                       (:fixed 'fixed-source-field))
                     field)))

(defun parse-cli-options (type options)
  "Parse options as per the WITH clause when we get them from the CLI."
  (alexandria:alist-plist
   (loop :for option :in options
      :collect (parse (case type
                        (:csv    'csv-option)
                        (:fixed  'fixed-option)
                        (:dbf    'dbf-option)
                        (:ixf    'ixf-option)
                        (:sqlite 'sqlite-option)
                        (:mysql  'mysql-option)
                        (:mssql  'mysql-option))
                      option))))

(defun parse-cli-casts (casts)
  "Parse additional CAST rules when we get them from the CLI."
  (loop :for cast :in casts
     :collect (parse 'cast-rule cast)))

(defun parse-sql-file (filename)
  "Parse FILENAME for SQL statements"
  (when filename
    (log-message :notice "reading SQL queries from ~s" filename)
    (pgloader.sql:read-queries (probe-file filename))))

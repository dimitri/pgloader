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
                          load-copy-file
			  load-dbf-file
                          load-ixf-file
                          load-pgsql-database
			  load-mysql-database
                          load-mssql-database
			  load-sqlite-database
			  ;; load-syslog-messages
                          )
		      end-of-command)
  (:lambda (cmd)
    (bind (((command _) cmd)) command)))

(defrule commands (+ command))

(defun parse-commands (commands-template &key (start 0) end junk-allowed)
  "Parse a command and return a LAMBDA form that takes no parameter."
  (let ((commands (apply-template (subseq commands-template start end))))
    (unless junk-allowed
      (log-message :info "Parsed command:~%~a~%" commands))
    (parse 'commands
           commands
           :start start
           :end end
           :junk-allowed junk-allowed)))

(defun inject-inline-data-position (command position)
  "We have '(:inline nil) somewhere in command, have '(:inline position) instead."
  (loop
     :for s-exp :in command

     :when (and (typep s-exp 'md-connection)
                (slot-boundp s-exp 'pgloader.sources::spec)
                (eq :inline (first (md-spec s-exp))))
     :do (setf (second (md-spec s-exp)) position)
     :and :collect s-exp

     :else :collect (if (and (consp s-exp) (listp (cdr s-exp)))
                        (inject-inline-data-position s-exp position)
                        s-exp)))

(defun process-relative-pathnames (filename command)
  "Walk the COMMAND to replace relative pathname with absolute ones, merging
   them within the directory where we found the command FILENAME."
  (loop
     :for s-exp :in command

     :collect (cond ((pathnamep s-exp)
                     (if (uiop:relative-pathname-p s-exp)
                         (uiop:merge-pathnames* s-exp filename)
                         s-exp))

                    ((and (typep s-exp 'fd-connection)
                          (slot-boundp s-exp 'pgloader.connection::path))
                     (when (uiop:relative-pathname-p (fd-path s-exp))
                       (setf (fd-path s-exp)
                             (uiop:merge-pathnames* (fd-path s-exp)
                                                    filename)))
                     s-exp)

                    ((and (typep s-exp 'md-connection)
                          (slot-boundp s-exp 'pgloader.sources::spec)
                          (eq :filename (car (md-spec s-exp))))
                     (let ((path (second (md-spec s-exp))))
                       (if (uiop:relative-pathname-p path)
                           (progn (setf (md-spec s-exp)
                                        `(:filename
                                          ,(uiop:merge-pathnames* path
                                                                  filename)))
                                  s-exp)
                           s-exp)))

                    (t
                     (if (and (consp s-exp) (listp (cdr s-exp)))
                         (process-relative-pathnames filename s-exp)
                         s-exp)))))

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
	 (parse-commands content :junk-allowed t)

       ;; INLINE is only allowed where we have a single command in the file
       (if *data-expected-inline*
	   (progn
	     (when (= 0 end-commands-position)
	       ;; didn't find any command, leave error reporting to esrap
	       (parse-commands content))

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
             (let ((command
                    (parse-commands content :end end-commands-position)))
               (list
	      (inject-inline-data-position (first command)
                                           (cons filename
                                                 end-commands-position)))))

	   ;; There was no INLINE magic found in the file, reparse it so that
	   ;; normal error processing happen
	   (parse-commands content))))))


;;;
;;; Parse an URI without knowing before hand what kind of uri it is.
;;;
(defvar *data-source-filename-extensions*
  '((:csv     . ("csv" "tsv" "txt" "text"))
    (:copy    . ("copy" "dat"))         ; reject data files are .dat
    (:sqlite  . ("sqlite" "db" "sqlite3"))
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
      (when (<= 2 (length dotted-parts))
        (destructuring-bind (ext name-or-ext &rest parts)
            dotted-parts
          (declare (ignore parts))
          (if (string-equal "tar" name-or-ext) :archive
              (loop :for (type . extensions) :in *data-source-filename-extensions*
                 :when (member ext extensions :test #'string-equal)
                 :return type)))))))

(defvar *parse-rule-for-source-types*
  '(:csv     csv-file-source
    :fixed   fixed-file-source
    :copy    copy-file-source
    :dbf     dbf-file-source
    :ixf     ixf-file-source
    :sqlite  sqlite-uri
    :pgsql   pgsql-uri
    :mysql   mysql-uri
    :mssql   mssql-uri)
  "A plist to associate source type and its source parsing rule.")

(defun parse-source-string-for-type (type source-string)
  "use the parse rules as per xxx-source rules"
  (parse (getf *parse-rule-for-source-types* type) source-string))

(defrule source-uri (or csv-uri
                        fixed-uri
                        copy-uri
                        dbf-uri
                        ixf-uri
                        sqlite-db-uri
                        pgsql-uri
                        mysql-uri
                        mssql-uri
                        filename-or-http-uri))

(defun parse-source-string (source-string)
  (let ((source (parse 'source-uri source-string)))
    (cond ((typep source 'connection)
           source)

          (t
           (destructuring-bind (kind url) source
             (let ((type
                    (case kind
                      (:filename (parse-filename-for-source-type url))
                      (:http     (parse-filename-for-source-type
                                  (puri:uri-path (puri:parse-uri url)))))))
               (when type
                 (parse-source-string-for-type type source-string))))))))

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
(defrule sqlite-type-name (or "sqlite3" "sqlite") (:constant "sqlite"))

(defrule cli-type (or "csv"
                      "fixed"
                      "copy"
                      dbf-type-name
                      sqlite-type-name
                      "ixf"
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
     :append (parse (case type
                      (:csv   'csv-source-fields)
                      (:fixed 'fixed-source-fields)
                      (:copy  'copy-source-fields))
                    field)))

(defun parse-cli-options (type options)
  "Parse options as per the WITH clause when we get them from the CLI."
  (alexandria:alist-plist
   (loop :for option :in options
      :collect (parse (ecase type
                        (:csv    'csv-option)
                        (:fixed  'fixed-option)
                        (:copy   'copy-option)
                        (:dbf    'dbf-option)
                        (:ixf    'ixf-option)
                        (:sqlite 'sqlite-option)
                        (:pgsql  'pgsql-option)
                        (:mysql  'mysql-option)
                        (:mssql  'mssql-option))
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


;;;
;;; Helper for regression testing
;;;
(defrule pg-db-uri-from-command (or pg-db-uri-from-files
                                    pg-db-uri-from-source-target
                                    pg-db-uri-from-source-table-target
                                    pg-db-uri-from-source-and-encoding))

(defrule pg-db-uri-from-files (or load-csv-file-command
                                  load-copy-file-command
                                  load-fixed-cols-file-command)
  (:lambda (command)
    (destructuring-bind (source encoding fields pg-db-uri table-name columns
                                &key gucs &allow-other-keys)
        command
      (declare (ignore source encoding fields columns))
      (list pg-db-uri table-name gucs))))

(defrule pg-db-uri-from-source-target (or load-sqlite-command
                                          load-mysql-command
                                          load-mssql-command)
  (:lambda (command)
    (destructuring-bind (source pg-db-uri &key gucs &allow-other-keys)
        command
      (declare (ignore source))
      (list pg-db-uri nil gucs))))

(defrule pg-db-uri-from-source-table-target (or load-ixf-command)
  (:lambda (command)
    (destructuring-bind (source pg-db-uri table-name &key gucs &allow-other-keys)
        command
      (declare (ignore source))
      (list pg-db-uri table-name gucs))))

(defrule pg-db-uri-from-source-and-encoding (or load-dbf-command)
  (:lambda (command)
    (destructuring-bind (source encoding pg-db-uri table-name
                                &key gucs &allow-other-keys)
        command
      (declare (ignore source encoding))
      (list pg-db-uri table-name gucs))))

(defun parse-target-pg-db-uri (command-file)
  "Partially parse COMMAND-FILE and return its target connection string."
  (let* ((content (read-file-into-string command-file)))

    (parse 'pg-db-uri-from-command content :junk-allowed t)))

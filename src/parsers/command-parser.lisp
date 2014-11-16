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
                       ((:summary summary-pathname))
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
      (let* ((summary-stream (when summary-pathname
                               (open summary-pathname
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
;;; Interactive tool
;;;
(defmacro with-database-uri ((database-uri) &body body)
  "Run the BODY forms with the connection parameters set to proper values
   from the DATABASE-URI. For a MySQL connection string, that's
   *myconn-user* and all, for a PostgreSQL connection string, *pgconn-user*
   and all."
  (destructuring-bind (&key type user password host port dbname
                            &allow-other-keys)
      (parse 'db-connection-uri database-uri)
    (ecase type
      (:mysql
       `(let* ((*myconn-host* ,(if (consp host) (list 'quote host) host))
	       (*myconn-port* ,port)
	       (*myconn-user* ,user)
	       (*myconn-pass* ,password)
               (*my-dbname*   ,dbname))
	  ,@body))
      (:postgresql
       `(let* ((*pgconn-host* ,(if (consp host) (list 'quote host) host))
	       (*pgconn-port* ,port)
	       (*pgconn-user* ,user)
	       (*pgconn-pass* ,password)
               (*pg-dbname*   ,dbname))
	  ,@body)))))

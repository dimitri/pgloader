(in-package #:pgloader)

;;;
;;; Some command line constants for OS errors codes
;;;
(defparameter +os-code-success+          0)
(defparameter +os-code-error+            1)
(defparameter +os-code-error-usage+      2)
(defparameter +os-code-error-bad-source+ 4)

;;;
;;; Now some tooling
;;;
(defun log-threshold (min-message &key quiet verbose debug)
  "Return the internal value to use given the script parameters."
  (cond ((and debug verbose) :data)
        (debug   :debug)
	(verbose :notice)
	(quiet   :error)
	(t       (or (find-symbol (string-upcase min-message) "KEYWORD")
		     :notice))))

(defparameter *opt-spec*
  `((("help" #\h) :type boolean :documentation "Show usage and exit.")

    (("version" #\V) :type boolean
     :documentation "Displays pgloader version and exit.")

    (("quiet"   #\q) :type boolean :documentation "Be quiet")
    (("verbose" #\v) :type boolean :documentation "Be verbose")
    (("debug"   #\d) :type boolean :documentation "Display debug level information.")

    ("client-min-messages" :type string :initial-value "warning"
			   :documentation "Filter logs seen at the console")

    ("log-min-messages" :type string :initial-value "notice"
			:documentation "Filter logs seen in the logfile")

    (("summary" #\S) :type string :documentation "Filename where to copy the summary")

    (("root-dir" #\D) :type string :initial-value ,*root-dir*
                      :documentation "Output root directory.")

    (("upgrade-config" #\U) :type boolean
     :documentation "Output the command(s) corresponding to .conf file for v2.x")

    (("list-encodings" #\E) :type boolean
     :documentation "List pgloader known encodings and exit.")

    (("logfile" #\L) :type string
     :documentation "Filename where to send the logs.")

    (("load-lisp-file" #\l) :type string :list t :optional t
     :documentation "Read user code from files")

    ("dry-run" :type boolean
     :documentation "Only check database connections, don't load anything.")

    (("with") :type string :list t :optional t
     :documentation "Load options")

    (("set") :type string :list t :optional t
     :documentation "PostgreSQL options")

    (("field") :type string :list t :optional t
     :documentation "Source file fields specification")

    (("cast") :type string :list t :optional t
     :documentation "Specific cast rules")

    (("type") :type string :optional t
     :documentation "Force input source type")

    (("encoding") :type string :optional t
     :documentation "Source expected encoding")

    (("before") :type string :optional t
     :documentation "SQL script to run before loading the data")

    (("after") :type string :optional t
     :documentation "SQL script to run after loading the data")

    ("self-upgrade" :type string :optional t
     :documentation "Path to pgloader newer sources")))

(defun print-backtrace (condition debug stream)
  "Depending on DEBUG, print out the full backtrace or just a shorter
   message on STREAM for given CONDITION."
  (if debug
      (trivial-backtrace:print-backtrace condition :output stream :verbose t)
      (trivial-backtrace:print-condition condition stream)))

(defun mkdir-or-die (path debug &optional (stream *standard-output*))
  "Create a directory at given PATH and exit with an error message when
   that's not possible."
  (handler-case
      (let ((dir (uiop:ensure-directory-pathname path)))
        (when debug
          (format stream "mkdir -p ~s~%" dir))
        (uiop:parse-unix-namestring (ensure-directories-exist dir)))
    (condition (e)
      ;; any error here is a panic
      (if debug
	  (print-backtrace e debug stream)
	  (format stream "PANIC: ~a.~%" e))
      (uiop:quit))))

(defun log-file-name (logfile)
  " If the logfile has not been given by the user, default to using
    pgloader.log within *root-dir*."
  (cond ((null logfile)
	 (make-pathname :defaults *root-dir*
			:name "pgloader"
			:type "log"))

	((fad:pathname-relative-p logfile)
	 (merge-pathnames logfile *root-dir*))

	(t
	 logfile)))

(defun usage (argv &key quit)
  "Show usage then QUIT if asked to."
  (format t "~&~a [ option ... ] command-file ..." (first argv))
  (format t "~&~a [ option ... ] SOURCE TARGET" (first argv))
  (command-line-arguments:show-option-help *opt-spec*)
  (when quit (uiop:quit +os-code-error-usage+)))

(defvar *self-upgraded-already* nil
  "Keep track if we did reload our own source code already.")

(defun self-upgrade (namestring &optional debug)
  "Load pgloader sources at PATH-TO-PGLOADER-SOURCES."
  (let ((pgloader-pathname (uiop:directory-exists-p
                            (uiop:parse-unix-namestring namestring))))
    (unless pgloader-pathname
      (format t "No such directory: ~s~%" namestring)
      (uiop:quit +os-code-error+))

    ;; now the real thing
    (handler-case
        (handler-bind ((warning #'muffle-warning))
          (let ((asdf:*central-registry* (list* pgloader-pathname
                                                asdf:*central-registry*)))
            (format t "Self-upgrading from sources at ~s~%"
                    (uiop:native-namestring pgloader-pathname))
            (with-output-to-string (*standard-output*)
              (asdf:load-system :pgloader
                                :verbose nil
                                :force-not *self-upgrade-immutable-systems*))))
      (condition (c)
        (format t "Fatal: ~a~%" c)
        (format t "~a~%" *self-upgrade-immutable-systems*)
        (when debug (invoke-debugger c))))))

(defun parse-summary-filename (summary debug)
  "Return the pathname where to write the summary output."
  (when summary
    (let* ((summary-pathname (uiop:parse-unix-namestring summary))
           (summary-pathname (if (uiop:absolute-pathname-p summary-pathname)
                                 summary-pathname
                                 (uiop:merge-pathnames* summary-pathname *root-dir*)))
           (summary-dir      (directory-namestring summary-pathname)))
      (mkdir-or-die summary-dir debug)
      summary-pathname)))

(defvar *--load-list-file-extension-whitelist* '("lisp" "lsp" "cl" "asd")
  "White list of file extensions allowed with the --load option.")

(defun load-extra-transformation-functions (filename)
  "Load an extra filename to tweak pgloader's behavior."
  (let ((pathname (uiop:parse-native-namestring filename)))
    (unless (member (pathname-type pathname)
                    *--load-list-file-extension-whitelist*
                    :test #'string=)
      (error "Unknown lisp file extension: ~s" (pathname-type pathname)))

    (log-message :info "Loading code from ~s" pathname)
    (load (compile-file pathname :verbose nil :print nil))))

(defun main (argv)
  "Entry point when building an executable image with buildapp"
  (let ((args (rest argv)))
    (multiple-value-bind (options arguments)
	(handler-case
            (command-line-arguments:process-command-line-options *opt-spec* args)
          (condition (e)
            ;; print out the usage, whatever happens here
            ;; (declare (ignore e))
            (format t "~a~%" e)
            (usage argv :quit t)))

      (destructuring-bind (&key help version quiet verbose debug logfile
				list-encodings upgrade-config dry-run
                                ((:load-lisp-file load))
				client-min-messages log-min-messages summary
				root-dir self-upgrade
                                with set field cast type encoding before after)
	  options

        ;; parse the log thresholds
        (setf *log-min-messages*
              (log-threshold log-min-messages
                             :quiet quiet :verbose verbose :debug debug)

              *client-min-messages*
              (log-threshold client-min-messages
                             :quiet quiet :verbose verbose :debug debug)

              verbose (member *client-min-messages* '(:info :debug :data))
              debug   (member *client-min-messages* '(:debug :data))
              quiet   (and (not verbose) (not debug)))

        ;; First thing: Self Upgrade?
        (when self-upgrade
          (unless *self-upgraded-already*
            (self-upgrade self-upgrade debug)
            (let ((*self-upgraded-already* t))
              (main argv))))

	;; First care about the root directory where pgloader is supposed to
	;; output its data logs and reject files
        (let ((root-dir-truename (or (probe-file root-dir)
                                     (mkdir-or-die root-dir debug))))
          (setf *root-dir* (uiop:ensure-directory-pathname root-dir-truename)))

	;; Set parameters that come from the environement
	(init-params-from-environment)

	;; Then process options
	(when debug
	  #+sbcl
          (format t "sb-impl::*default-external-format* ~s~%"
		  sb-impl::*default-external-format*)
	  (format t "tmpdir: ~s~%" *default-tmpdir*))

	(when version
	  (format t "pgloader version ~s~%" *version-string*)
          (format t "compiled with ~a ~a~%"
                  (lisp-implementation-type)
                  (lisp-implementation-version)))

	(when help
          (usage argv))

	(when (or help version) (uiop:quit +os-code-success+))

	(when list-encodings
	  (show-encodings)
	  (uiop:quit +os-code-success+))

	(when upgrade-config
	  (loop for filename in arguments
	     do
               (handler-case
                   (with-monitor ()
                     (pgloader.ini:convert-ini-into-commands filename))
                 (condition (c)
                   (when debug (invoke-debugger c))
                   (uiop:quit +os-code-error+))))
	  (uiop:quit +os-code-success+))

        ;; Should we run in dry-run mode?
        (setf *dry-run* dry-run)

	;; Now process the arguments
	(when arguments
	  ;; Start the logs system
	  (let* ((*log-filename*      (log-file-name logfile))
                 (*summary-pathname*  (parse-summary-filename summary debug)))

            (with-monitor ()
              ;; tell the user where to look for interesting things
              (log-message :log "Main logs in '~a'"
                           (uiop:native-namestring *log-filename*))
              (log-message :log "Data errors in '~a'~%" *root-dir*)

              ;; load extra lisp code provided for by the user
              (when load
                (loop for filename in load do
                     (handler-case
                         (load-extra-transformation-functions filename)
                       (condition (e)
                         (log-message :fatal
                                      "Failed to load lisp source file ~s~%"
                                      filename)
                         (log-message :error "~a" e)
                         (uiop:quit +os-code-error+)))))

              (handler-case
                  ;; The handler-case is to catch unhandled exceptions at the
                  ;; top level.
                  ;;
                  ;; The handler-bind is to be able to offer a meaningful
                  ;; backtrace to the user in case of unexpected conditions
                  ;; being signaled.
                  (handler-bind
                      ((condition
                        #'(lambda (condition)
                            (log-message :fatal "We have a situation here.")
                            (print-backtrace condition debug *standard-output*))))

                    (if (= 2 (length arguments))
                        ;; if there are exactly two arguments in the command
                        ;; line, try and process them as source and target
                        ;; arguments
                        (process-source-and-target (first arguments)
                                                   (second arguments)
                                                   type encoding
                                                   set with field cast
                                                   before after)

                        ;; process the files
                        (mapcar #'process-command-file arguments)))

                (source-definition-error (c)
                  (log-message :fatal "~a" c)
                  (uiop:quit +os-code-error-bad-source+))

                (condition (c)
                  (when debug (invoke-debugger c))
                  (uiop:quit +os-code-error+))))))

        ;; done.
	(uiop:quit +os-code-success+)))))

(defun process-command-file (filename)
  "Process FILENAME as a pgloader command file (.load)."
  (let ((truename (probe-file filename)))
    (if truename
        (run-commands truename :start-logger nil)
        (log-message :error "Can not find file: ~s" filename)))
  (format t "~&"))

(defun process-source-and-target (source target
                                  type encoding set with field cast
                                  before after)
  "Given exactly 2 CLI arguments, process them as source and target URIs."
  (let* ((type       (parse-cli-type type))
         (source-uri (if type
                         (parse-source-string-for-type type source)
                         (parse-source-string source)))
         (type       (when source
                       (parse-cli-type (conn-type source))))
         (target-uri (parse-target-string target)))

    ;; some verbosity about the parsing "magic"
    (log-message :info "SOURCE: ~s" source)
    (log-message :info "TARGET: ~s" target)

    (cond ((and (null source-uri)
                (null target-uri)
                (probe-file (uiop:parse-unix-namestring source))
                (probe-file (uiop:parse-unix-namestring target)))
           (mapcar #'process-command-file (list source target)))

          ((null source)
           (log-message :fatal
                        "Failed to parse ~s as a source URI." source)
           (log-message :log "You might need to use --type."))

          ((null target)
           (log-message :fatal
                        "Failed to parse ~s as a PostgreSQL database URI."
                        target)))

    ;; so, we actually have all the specs for the
    ;; job on the command line now.
    (when (and source-uri target-uri)
      (load-data :from source-uri
                 :into target-uri
                 :encoding (parse-cli-encoding encoding)
                 :options  (parse-cli-options type with)
                 :gucs     (parse-cli-gucs set)
                 :fields   (parse-cli-fields type field)
                 :casts    (parse-cli-casts cast)
                 :before   (parse-sql-file before)
                 :after    (parse-sql-file after)
                 :start-logger nil))))

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

      (loop for func in funcs do (funcall func)))))


;;;
;;; Main API to use from outside of pgloader.
;;;
(define-condition source-definition-error (error)
  ((mesg :initarg :mesg :reader source-definition-error-mesg))
  (:report (lambda (err stream)
             (format stream "~a" (source-definition-error-mesg err)))))

(defun load-data (&key ((:from source)) ((:into target))
                    encoding fields options gucs casts before after
                    (start-logger t))
  "Load data from SOURCE into TARGET."
  (declare (type connection source)
           (type pgsql-connection target))

  ;; some preliminary checks
  (when (and (typep source 'csv-connection)
             (not (typep source 'copy-connection))
             (null fields))
    (error 'source-definition-error
           :mesg "This data source requires fields definitions."))

  (when (and (typep source 'csv-connection) (null (pgconn-table-name target)))
    (error 'source-definition-error
           :mesg "This data source require a table name target."))

  (when (and (typep source 'fixed-connection) (null (pgconn-table-name target)))
    (error 'source-definition-error
           :mesg "Fixed-width data source require a table name target."))

  (with-monitor (:start-logger start-logger)
    (when (and casts (not (member (type-of source)
                                  '(sqlite-connection
                                    mysql-connection
                                    mssql-connection))))
      (log-message :log "Cast rules are ignored for this sources."))

    ;; now generates the code for the command
    (log-message :debug "LOAD DATA FROM ~s" source)
    (run-commands
     (process-relative-pathnames
      (uiop:getcwd)
      (typecase source
        (copy-connection
         (lisp-code-for-loading-from-copy source fields target
                                          :encoding (or encoding :default)
                                          :gucs gucs
                                          :copy-options options
                                          :before before
                                          :after after))

        (fixed-connection
         (lisp-code-for-loading-from-fixed source fields target
                                           :encoding encoding
                                           :gucs gucs
                                           :fixed-options options
                                           :before before
                                           :after after))

        (csv-connection
         (lisp-code-for-loading-from-csv source fields target
                                         :encoding encoding
                                         :gucs gucs
                                         :csv-options options
                                         :before before
                                         :after after))

        (dbf-connection
         (lisp-code-for-loading-from-dbf source target
                                         :gucs gucs
                                         :dbf-options options
                                         :before before
                                         :after after))

        (ixf-connection
         (lisp-code-for-loading-from-ixf source target
                                         :gucs gucs
                                         :ixf-options options
                                         :before before
                                         :after after))

        (sqlite-connection
         (lisp-code-for-loading-from-sqlite source target
                                            :gucs gucs
                                            :casts casts
                                            :sqlite-options options))

        (mysql-connection
         (lisp-code-for-loading-from-mysql source target
                                           :gucs gucs
                                           :casts casts
                                           :mysql-options options
                                           :before before
                                           :after after))

        (mssql-connection
         (lisp-code-for-loading-from-mssql source target
                                           :gucs gucs
                                           :casts casts
                                           :mssql-options options
                                           :before before
                                           :after after))))
     :start-logger nil)))

(in-package #:pgloader)

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

    ("on-error-stop" :type boolean
                     :documentation "Refrain from handling errors properly.")

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
                    :documentation "Path to pgloader newer sources")

    ("regress" :type boolean :optional t
               :documentation "Drive regression testing")))

(defun print-backtrace (condition debug)
  "Depending on DEBUG, print out the full backtrace or just a shorter
   message on STREAM for given CONDITION."
  (if debug
      (trivial-backtrace:print-backtrace condition :output nil)
      (trivial-backtrace:print-condition condition nil)))

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
	  (format stream "PANIC: ~a~%" (print-backtrace e debug))
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

(defun load-extra-transformation-functions (filename &optional verbose)
  "Load an extra filename to tweak pgloader's behavior."
  (let ((pathname (uiop:parse-native-namestring filename)))
    (unless (member (pathname-type pathname)
                    *--load-list-file-extension-whitelist*
                    :test #'string=)
      (error "Unknown lisp file extension: ~s" (pathname-type pathname)))

    (format t "Loading code from ~s~%" pathname)
    (load (compile-file pathname :verbose verbose :print verbose))))

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
				list-encodings upgrade-config
                                dry-run on-error-stop
                                ((:load-lisp-file load))
				client-min-messages log-min-messages summary
				root-dir self-upgrade
                                with set field cast type encoding before after
                                regress)
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

        ;; Should we stop at first error?
        (setf *on-error-stop* on-error-stop)

        ;; load extra lisp code provided for by the user
        (when load
          (loop :for filename :in load :do
             (handler-case
                 (load-extra-transformation-functions filename debug)
               ((or simple-condition serious-condition) (e)
                 (format *error-output*
                         "Failed to load lisp source file ~s~%" filename)
                 (format *error-output* "~a~%~%" e)
                 (uiop:quit +os-code-error+)))))

	;; Now process the arguments
	(when arguments
	  ;; Start the logs system
	  (let* ((*log-filename*      (log-file-name logfile))
                 (*summary-pathname*  (parse-summary-filename summary debug)))

            (handler-case
                ;; The handler-case is to catch unhandled exceptions at the
                ;; top level.
                ;;
                ;; The handler-bind below is to be able to offer a
                ;; meaningful backtrace to the user in case of unexpected
                ;; conditions being signaled.
                (handler-bind
                    (((and condition (not (or cli-parsing-error
                                              source-definition-error)))
                      #'(lambda (condition)
                          (format *error-output* "KABOOM!~%")
                          (format *error-output* "FATAL error: ~a~%~a~%~%"
                                  condition
                                  (print-backtrace condition debug)))))

                  (with-monitor ()
                    ;; tell the user where to look for interesting things
                    (log-message :log "Main logs in '~a'"
                                 (uiop:native-namestring *log-filename*))
                    (log-message :log "Data errors in '~a'~%" *root-dir*)

                    (cond
                      ((and regress (= 1 (length arguments)))
                       ;; run a regression test
                       (process-regression-test (first arguments)))

                      (regress
                       (log-message :fatal "Regression testing requires a single .load file as input."))

                      ((= 2 (length arguments))
                       ;; if there are exactly two arguments in the command
                       ;; line, try and process them as source and target
                       ;; arguments
                       (process-source-and-target (first arguments)
                                                  (second arguments)
                                                  type encoding
                                                  set with field cast
                                                  before after))
                      (t
                       ;; process the files
                       ;; other options are not going to be used here
                       (let ((cli-options `(("--type"     ,type)
                                            ("--encoding" ,encoding)
                                            ("--set"      ,set)
                                            ("--with"     ,with)
                                            ("--field"    ,field)
                                            ("--cast"     ,cast)
                                            ("--before"   ,before)
                                            ("--after"    ,after))))
                         (loop :for (cli-option-name cli-option-value)
                            :in cli-options
                            :when cli-option-value
                            :do (log-message
                                 :fatal
                                 "Option ~s is ignored when using a load file"
                                 cli-option-name))

                         ;; when we issued a single error previously, do nothing
                         (unless (remove-if #'null (mapcar #'second cli-options))
                           (process-command-file arguments)))))))

              ((or cli-parsing-error source-definition-error) (c)
                (format *error-output* "~%~a~%~%" c)
                (let ((lp:*kernel* *monitoring-kernel*))
                  (lp:end-kernel :wait t))
                (uiop:quit +os-code-error-bad-source+))

              (condition (c)
                (format *error-output* "~%What I am doing here?~%~%")
                (format *error-output* "~a~%~%" c)
                ;; wait until monitor stops...
                (format *error-output*
                        "~%Waiting for the monitor thread to complete.~%~%")
                (let ((lp:*kernel* *monitoring-kernel*))
                  (lp:end-kernel :wait t))
                (uiop:quit +os-code-error+)))))

        ;; done.
	(uiop:quit +os-code-success+)))))


;;;
;;; Helper functions to actually do things
;;;
(define-condition load-files-not-found-error (error)
  ((filename-list :initarg :filename-list))
  (:report (lambda (err stream)
             (format stream
                     ;; start lines with 3 spaces because of trivial-backtrace
                     "~{No such file or directory: ~s~^~%   ~}"
                     (slot-value err 'filename-list)))))

(defun process-command-file (filename-list &key (flush-summary t))
  "Process each FILENAME in FILENAME-LIST as a pgloader command
   file (.load)."
  (loop :for filename :in filename-list
     :for truename := (probe-file filename)
     :unless truename :collect filename :into not-found-list
     :do (if truename
             (run-commands truename
                           :start-logger nil
                           :flush-summary flush-summary)
             (log-message :error "Can not find file: ~s" filename))
     :finally (when not-found-list
                (error 'load-files-not-found-error :filename-list not-found-list))))

(define-condition cli-parsing-error (error) ()
  (:report (lambda (err stream)
             (declare (ignore err))
             (format stream "Could not parse the command line: see above."))))

(defun process-source-and-target (source-string target-string
                                  type encoding set with field cast
                                  before after)
  "Given exactly 2 CLI arguments, process them as source and target URIs.
Parameters here are meant to be already parsed, see parse-cli-optargs."
  (let* ((type       (handler-case
                         (parse-cli-type type)
                       (condition (e)
                         (log-message :warning
                                      "Could not parse --type ~s: ~a"
                                      type e))))
         (source-uri (handler-case
                         (if type
                             (parse-source-string-for-type type source-string)
                             (parse-source-string source-string))
                       (condition (e)
                         (log-message :warning
                                      "Could not parse source string ~s: ~a"
                                      source-string e))))
         (type       (when (and source-string
                                (typep source-uri 'connection))
                       (parse-cli-type (conn-type source-uri))))
         (target-uri (handler-case
                         (parse-target-string target-string)
                       (condition (e)
                         (log-message :error
                                      "Could not parse target string ~s: ~a"
                                      target-string e)))))

    ;; some verbosity about the parsing "magic"
    (log-message :info "    SOURCE: ~s" source-string)
    (log-message :info "SOURCE URI: ~s" source-uri)
    (log-message :info "    TARGET: ~s" target-string)
    (log-message :info "TARGET URI: ~s" target-uri)

    (cond ((and (null source-uri) (null target-uri))
           (process-command-file (list source-string target-string)))

          ((or (null source-string) (null source-uri))
           (log-message :fatal
                        "Failed to parse ~s as a source URI." source-string)
           (log-message :log "You might need to use --type."))

          ((or (null target-string) (null target-uri))
           (log-message :fatal
                        "Failed to parse ~s as a PostgreSQL database URI."
                        target-string)))

    (let* ((nb-errors 0)
           (options (handler-case
                        (parse-cli-options type with)
                      (condition (e)
                        (incf nb-errors)
                        (log-message :error "Could not parse --with ~s:" with)
                        (log-message :error "~a" e))))
           (fields  (handler-case
                        (parse-cli-fields type field)
                      (condition (e)
                        (incf nb-errors)
                        (log-message :error "Could not parse --fields ~s:" field)
                        (log-message :error "~a" e)))))

      (destructuring-bind (&key encoding gucs casts before after)
          (loop :for (keyword option user-string parse-fn)
             :in `((:encoding  "--encoding" ,encoding ,#'parse-cli-encoding)
                   (:gucs      "--set"      ,set      ,#'parse-cli-gucs)
                   (:casts     "--cast"     ,cast     ,#'parse-cli-casts)
                   (:before    "--before"   ,before   ,#'parse-sql-file)
                   (:after     "--after"    ,after    ,#'parse-sql-file))
             :append (list keyword
                           (handler-case
                               (funcall parse-fn user-string)
                             (condition (e)
                               (incf nb-errors)
                               (log-message :error "Could not parse ~a ~s: ~a"
                                            option user-string e)))))

        (unless (= 0 nb-errors)
          (error 'cli-parsing-error))

        ;; so, we actually have all the specs for the
        ;; job on the command line now.
        (when (and source-uri target-uri (= 0 nb-errors))
          (load-data :from source-uri
                     :into target-uri
                     :encoding encoding
                     :options  options
                     :gucs     gucs
                     :fields   fields
                     :casts    casts
                     :before   before
                     :after    after
                     :start-logger nil))))))


;;;
;;; Helper function to run a given command
;;;
(defun run-commands (source
		     &key
		       (start-logger t)
                       (flush-summary t)
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

      (loop :for func :in funcs
         :do (funcall func)
         :do (when flush-summary
               (flush-summary :reset t))))))


;;;
;;; Main API to use from outside of pgloader.
;;;
(define-condition source-definition-error (error)
  ((mesg :initarg :mesg :reader source-definition-error-mesg))
  (:report (lambda (err stream)
             (format stream "~a" (source-definition-error-mesg err)))))

(defun load-data (&key ((:from source)) ((:into target))
                    encoding fields options gucs casts before after
                    (start-logger t) (flush-summary t))
  "Load data from SOURCE into TARGET."
  (declare (type connection source)
           (type pgsql-connection target))

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
                                          :options options
                                          :before before
                                          :after after))

        (fixed-connection
         (lisp-code-for-loading-from-fixed source fields target
                                           :encoding encoding
                                           :gucs gucs
                                           :options options
                                           :before before
                                           :after after))

        (csv-connection
         (lisp-code-for-loading-from-csv source fields target
                                         :encoding encoding
                                         :gucs gucs
                                         :options options
                                         :before before
                                         :after after))

        (dbf-connection
         (lisp-code-for-loading-from-dbf source target
                                         :gucs gucs
                                         :options options
                                         :before before
                                         :after after))

        (ixf-connection
         (lisp-code-for-loading-from-ixf source target
                                         :gucs gucs
                                         :options options
                                         :before before
                                         :after after))

        (sqlite-connection
         (lisp-code-for-loading-from-sqlite source target
                                            :gucs gucs
                                            :casts casts
                                            :options options
                                            :before before
                                            :after after))

        (mysql-connection
         (lisp-code-for-loading-from-mysql source target
                                           :gucs gucs
                                           :casts casts
                                           :options options
                                           :before before
                                           :after after))

        (mssql-connection
         (lisp-code-for-loading-from-mssql source target
                                           :gucs gucs
                                           :casts casts
                                           :options options
                                           :before before
                                           :after after))))
     :start-logger nil
     :flush-summary flush-summary)))

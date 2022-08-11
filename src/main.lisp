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

    ("no-ssl-cert-verification"
     :type boolean
     :documentation "Instruct OpenSSL to bypass verifying certificates.")

    (("context" #\C) :type string :documentation "Command Context Variables")

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
                                dry-run on-error-stop context
                                ((:load-lisp-file load))
				client-min-messages log-min-messages summary
				root-dir self-upgrade
                                with set field cast type encoding before after
                                no-ssl-cert-verification
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

        ;; --list-encodings, -E
	(when list-encodings
	  (show-encodings)
	  (uiop:quit +os-code-success+))

	;; First care about the root directory where pgloader is supposed to
	;; output its data logs and reject files
        (let ((root-dir-truename (or (probe-file root-dir)
                                     (mkdir-or-die root-dir debug))))
          (setf *root-dir* (uiop:ensure-directory-pathname root-dir-truename)))

	;; Set parameters that come from the environement
	(init-params-from-environment)

        ;; Read the context file (if given) and the environment
        (handler-case
            (initialize-context context)
          (condition (e)
            (format t "Couldn't read ini file ~s: ~a~%" context e)
            (usage argv)))

	;; Then process options
	(when debug
          (format t "pgloader version ~a~%" *version-string*)
          #+pgloader-image
          (format t "compiled with ~a ~a~%"
                  (lisp-implementation-type)
                  (lisp-implementation-version))
	  #+sbcl
          (format t "sb-impl::*default-external-format* ~s~%"
		  sb-impl::*default-external-format*)
	  (format t "tmpdir: ~s~%" *default-tmpdir*))

	(when version
	  (format t "pgloader version ~s~%" *version-string*)
          (format t "compiled with ~a ~a~%"
                  (lisp-implementation-type)
                  (lisp-implementation-version)))

	(when (or help)
          (usage argv))

	(when (or help version) (uiop:quit +os-code-success+))

        (when (null arguments)
          (usage argv)
          (uiop:quit +os-code-error-usage+))

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
                    (((and serious-condition (not (or monitor-error
                                                      cli-parsing-error
                                                      source-definition-error
                                                      regression-test-error)))
                       #'(lambda (condition)
                           (format *error-output* "KABOOM!~%")
                           (format *error-output* "~a: ~a~%~a~%~%"
                                   (class-name (class-of condition))
                                   condition
                                   (print-backtrace condition debug)))))

                  (with-monitor ()
                    ;; tell the user where to look for interesting things
                    (log-message :log "Main logs in '~a'"
                                 (uiop:native-namestring *log-filename*))
                    (log-message :log "Data errors in '~a'~%" *root-dir*)

                    (when no-ssl-cert-verification
                      (setf cl+ssl:*make-ssl-client-stream-verify-default* nil))

                    (cond
                      ((and regress (= 1 (length arguments)))
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
                (uiop:quit +os-code-error-bad-source+))

              (regression-test-error (c)
                (format *error-output* "~%~a~%~%" c)
                (uiop:quit +os-code-error-regress+))

              (monitor-error (c)
                (format *error-output* "~a~%" c)
                (uiop:quit +os-code-error+))

              (serious-condition (c)
                (format *error-output* "~%What I am doing here?~%~%")
                (format *error-output* "~a~%~%" c)
                (uiop:quit +os-code-error+)))))

        ;; done.
	(uiop:quit +os-code-success+)))))

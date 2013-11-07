(in-package #:pgloader)

(defun list-encodings ()
  "List known encodings names and aliases from charsets::*lisp-encodings*."
  (format *standard-output* "Name    ~30TAliases~%")
  (format *standard-output* "--------~30T--------------~%")
  (loop
     with encodings = (sort (copy-tree charsets::*lisp-encodings*) #'string<
			    :key #'car)
     for (name . aliases) in encodings
     do (format *standard-output* "~a~30T~{~a~^, ~}~%" name aliases))
  (terpri))

(defparameter *opt-spec*
  `((("help" #\h) :type boolean :documentation "Show usage and exit.")

    (("version" #\V) :type boolean
     :documentation "Displays pgloader version and exit.")

    (("quiet"   #\q) :type boolean :documentation "Be quiet")
    (("verbose" #\v) :type boolean :documentation "Be verbose")
    (("debug"   #\d) :type boolean :documentation "Diplay debug level information.")

    (("upgrade-config" #\U) :type boolean
     :documentation "Output the command(s) corresponding to .conf file for v2.x")

    (("list-encodings" #\E) :type boolean
     :documentation "List pgloader known encodings and exit.")

    (("logfile" #\L) :type string :initial-value "/tmp/pgloader/pgloader.log"
     :documentation "Filename where to send the logs.")

    (("load" #\l) :type string :list t :optional t
     :documentation "Read user code from file")))

(defun main (argv)
  "Entry point when building an executable image with buildapp"
  (let ((args (rest argv)))
    (multiple-value-bind (options arguments)
	(command-line-arguments:process-command-line-options *opt-spec* args)

      (destructuring-bind (&key help version quiet verbose debug logfile
				list-encodings upgrade-config load)
	  options

	(when version
	  (format t "pgloader version ~s~%" *version-string*))

	(when help
	  (command-line-arguments:show-option-help *opt-spec*))

	(when (or help version) (uiop:quit))

	(when list-encodings
	  (list-encodings)
	  (uiop:quit))

	(when upgrade-config
	  (loop for filename in arguments
	     do
	       (pgloader.ini:convert-ini-into-commands filename)
	       (format t "~%~%"))
	  (uiop:quit))

	(when load
	  (loop for filename in load
	     do (load (compile-file filename :verbose nil :print nil))))

	(when arguments
	  ;; process the files
	  (handler-bind
	      ((condition
		#'(lambda (c)
		    (if debug
			(trivial-backtrace:print-backtrace c
							   :output *standard-output*
							   :verbose t)
			(trivial-backtrace:print-condition c *standard-output*)))))
	      (let ((min-messages (cond (debug   :debug)
					(verbose :info)
					(quiet   :warning)
					(t       :notice))))
		(loop for filename in arguments
		   do
		     (run-commands filename
				   :log-filename logfile
				   :log-min-messages min-messages
				   :client-min-messages min-messages)
		     (format t "~&")))))

	(uiop:quit)))))

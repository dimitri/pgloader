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

    (("quiet" #\q)   :type boolean :documentation "Be quiet")
    (("verbose" #\v) :type boolean :documentation "Be verbose")
    (("debug"   #\d) :type boolean :documentation "Diplay debug level information.")

    (("list-encodings" #\E) :type boolean
     :documentation "List pgloader known encodings and exit.")

    (("load" #\l) :type string :list t :optional t
     :documentation "Read user code from file")))

(defun main (argv)
  "Entry point when building an executable image with buildapp"
  (let ((args (rest argv)))
    (multiple-value-bind (options arguments)
	(command-line-arguments:process-command-line-options *opt-spec* args)

      (destructuring-bind (&key help version quiet verbose debug
				list-encodings load)
	  options

	(when version
	  (format t "pgloader version ~s~%" *version-string*))

	(when help
	  (command-line-arguments:show-option-help *opt-spec*))

	(when (or help version) (uiop:quit))

	(when list-encodings
	  (list-encodings)
	  (uiop:quit))

	(setf *client-min-messages* (cond (debug   :debug)
					  (verbose :info)
					  (quiet   :warning)
					  (t       :notice)))

	(when load
	  (loop for filename in load
	     do (load (compile-file filename :verbose nil :print nil))))

	(when arguments
	  ;; Start the logger
	  (start-logger)

	  ;; process the files
	  (handler-case
	      (loop for filename in arguments
		 do
		   (log-message :notice "Processing ~s~%" filename)
		   (run-commands filename)
		   (format t "~&"))
	    (condition (e)
	      (if debug (error e) (format *standard-output* "~a" e)))))

	(uiop:quit)))))

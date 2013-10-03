(in-package #:pgloader)

(defparameter *opt-spec*
  `((("help" #\h) :type boolean :documentation "show usage")
    (("file" #\f) :type string :documentation "read commands from file")))

(defun main (argv)
  "Entry point when building an executable image with buildapp"
  (multiple-value-bind (options arguments)
      (command-line-arguments:process-command-line-options *opt-spec* argv)
    (declare (ignore arguments))
    (destructuring-bind (&key help file) options

      (when help
	(command-line-arguments:show-option-help *opt-spec*)
	(uiop:quit))

      (run-command (slurp-file-into-string file))
      (format t "~&")

      (uiop:quit))))

#! /usr/local/bin/sbcl --script

;;; load the necessary components then parse the command line
;;; and launch the work

#-quicklisp
(let ((quicklisp-init (merge-pathnames "quicklisp/setup.lisp"
                                       (user-homedir-pathname))))
  (when (probe-file quicklisp-init)
    (load quicklisp-init)))

;; now is the time to load our Quicklisp project
(format t "Loading quicklisp and the pgloader project and its dependencies...")
(terpri)
(with-output-to-string (*standard-output*)
  (pushnew :lparallel.with-vector-queue *features*)
  (pushnew :lparallel.without-stealing-scheduler *features*)
  (ql:quickload '(:pgloader)))

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

;;; actually call the main function, too
(main (uiop:command-line-arguments))

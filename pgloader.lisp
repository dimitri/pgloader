#!/bin/sh
#|
exec sbcl --script "$0" $@
|#

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
  (ql:quickload '(:pgloader)))

(in-package #:pgloader)

;;; actually call the main function, too
(main SB-EXT:*POSIX-ARGV*)

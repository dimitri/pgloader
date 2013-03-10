;;;
;;; pgloader parameters
;;;
;;; in a separate file to break circular dependencies

(defpackage #:pgloader.params
  (:use #:cl)
  (:export #:*csv-path-root*
	   #:*reject-path-root*
	   #:*loader-kernel*
	   #:*myconn-host*
	   #:*myconn-user*
	   #:*myconn-pass*
	   #:*state*))

(in-package :pgloader.params)

;; we can't use pgloader.utils:make-pgstate yet because params is compiled
;; first in the asd definition, we just make the symbol a special variable.
(defparameter *state* nil
  "State of the current loading.")

(defparameter *csv-path-root*
  (merge-pathnames "csv/" (user-homedir-pathname)))

(defparameter *reject-path-root*
  (make-pathname :directory "/tmp/pgloader/"))

;;; package nicknames are only defined later, in package.lisp
(defparameter *loader-kernel* (lparallel:make-kernel 2)
  "lparallel kernel to use for loading data in parallel")

(defparameter *myconn-host* "myhost")
(defparameter *myconn-user* "myuser")
(defparameter *myconn-pass* "mypass")


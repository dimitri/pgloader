;;;
;;; pgloader parameters
;;;
;;; in a separate file to break circular dependencies

(defpackage #:pgloader.params
  (:use #:cl)
  (:export #:*version-string*
           #:*self-upgrade-immutable-systems*
	   #:*csv-path-root*
	   #:*root-dir*
	   #:*log-filename*
           #:*summary-pathname*
	   #:*client-min-messages*
	   #:*log-min-messages*
           #:*report-stream*
           #:*identifier-case*
	   #:*copy-batch-rows*
           #:*copy-batch-size*
           #:*concurrent-batches*
	   #:*pg-settings*
	   #:*state*
	   #:*default-tmpdir*
	   #:init-params-from-environment
	   #:getenv-default))

(in-package :pgloader.params)

(defparameter *release* nil
  "non-nil when this build is a release build.")

(defparameter *major-version* "3.2")
(defparameter *minor-version* "1")

(defun git-hash ()
  "Return the current abbreviated git hash of the development tree."
  (handler-case
      (let ((git-hash `("git" "--no-pager" "log" "-n1" "--format=format:%h")))
        (uiop:with-current-directory ((asdf:system-source-directory :pgloader))
          (multiple-value-bind (stdout stderr code)
              (uiop:run-program git-hash :output :string)
            (declare (ignore code stderr))
            stdout)))
    (condition (e)
      ;; in case anything happen, just return X.Y.Z~devel
      (declare (ignore e))
      (format nil "~a~~devel" *minor-version*))))

(defparameter *version-string*
  (concatenate 'string *major-version* "."
               (if *release* *minor-version* (git-hash)))
  "pgloader version strings, following Emacs versionning model.")

(defvar *self-upgrade-immutable-systems* nil
  "Used for --self-upgrade.")

;;;
;;; We need that to setup our default connection parameters
;;;
(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun getenv-default (name &optional default)
    "Return the value of the NAME variable as found in the environment, or
     DEFAULT if that variable isn't set"
    (or (uiop:getenv name) default)))

;; we can't use pgloader.utils:make-pgstate yet because params is compiled
;; first in the asd definition, we just make the symbol a special variable.
(defparameter *state* nil
  "State of the current loading.")

(defparameter *csv-path-root* nil
  "Where to load CSV files from, when loading from an archive.")

(defparameter *root-dir*
  #+unix (make-pathname :directory "/tmp/pgloader/")
  #-unix (uiop:merge-pathnames*
          "pgloader/"
          (uiop:ensure-directory-pathname (getenv-default "Temp")))
  "Top directory where to store all data logs and reject files.")

(defparameter *log-filename*
  (make-pathname :defaults *root-dir*
                 :name "pgloader"
                 :type "log")
  "Main pgloader log file")

(defparameter *summary-pathname* nil "Pathname where to output the summary.")

(defparameter *client-min-messages* :notice)
(defparameter *log-min-messages* :info)

(defparameter *report-stream* *terminal-io*
  "Stream where to format the output stream.")

;;;
;;; When converting from other databases, how to deal with case sensitivity?
;;;
(defparameter *identifier-case* :downcase
  "Dealing with source databases casing rules.")

;;;
;;; How to split batches in case of data loading errors.
;;;
(defparameter *copy-batch-rows* 25000
  "How many rows to batch per COPY transaction.")

(defparameter *copy-batch-size* (* 20 1024 1024)
  "Maximum memory size allowed for a single batch.")

(defparameter *concurrent-batches* 10
  "How many batches do we stack in the queue in advance.")

(defparameter *pg-settings* nil "An alist of GUC names and values.")

;;;
;;; Archive processing: downloads and unzip.
;;;
(defparameter *default-tmpdir*
  (let* ((tmpdir (uiop:getenv "TMPDIR"))
	 (tmpdir (or (and tmpdir (probe-file tmpdir))
                     #+unix #P"/tmp/"
                     #-unix (uiop:ensure-directory-pathname
                             (getenv-default "Temp")))))
    (uiop:ensure-directory-pathname (merge-pathnames "pgloader" tmpdir)))
  "Place where to fetch and expand archives on-disk.")

;;;
;;; Run time initialisation of ENV provided parameters
;;;
;;; The command parser dynamically inspect the environment when building the
;;; connection parameters, so that we don't need to provision for those here.
;;;
(defun init-params-from-environment ()
  "Some of our parameters get their default value from the env. Do that at
   runtime when using a compiled binary."
  (setf *default-tmpdir*
	(fad:pathname-as-directory
	 (getenv-default "TMPDIR" *default-tmpdir*))))

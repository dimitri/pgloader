;;;
;;; pgloader parameters
;;;
;;; in a separate file to break circular dependencies

(defpackage #:pgloader.params
  (:use #:cl)
  (:export #:*version-string*
	   #:*csv-path-root*
	   #:*root-dir*
	   #:*log-filename*
	   #:*client-min-messages*
	   #:*log-min-messages*
           #:*report-stream*
           #:*identifier-case*
	   #:*copy-batch-rows*
           #:*copy-batch-size*
           #:*concurrent-batches*
	   #:*pgconn-host*
	   #:*pgconn-port*
	   #:*pgconn-user*
	   #:*pgconn-pass*
	   #:*pg-dbname*
	   #:*pg-settings*
	   #:*myconn-host*
	   #:*myconn-port*
	   #:*myconn-user*
	   #:*myconn-pass*
	   #:*my-dbname*
	   #:*msconn-host*
	   #:*msconn-port*
	   #:*msconn-user*
	   #:*msconn-pass*
	   #:*ms-dbname*
	   #:*state*
	   #:*default-tmpdir*
	   #:init-params-from-environment
	   #:getenv-default))

(in-package :pgloader.params)

(defparameter *release* nil
  "non-nil when this build is a release build.")

(defparameter *major-version* "3.1")
(defparameter *minor-version* "3")

(defun git-hash ()
  "Return the current abbreviated git hash of the development tree."
  (let ((git-hash `("git" "--no-pager" "log" "-n1" "--format=format:%h")))
    (uiop:with-current-directory ((asdf:system-source-directory :pgloader))
      (multiple-value-bind (stdout stderr code)
          (uiop:run-program git-hash :output :string)
        (declare (ignore code stderr))
        stdout))))

(defparameter *version-string*
  (concatenate 'string *major-version* "."
               (if *release* *minor-version* (git-hash)))
  "pgloader version strings, following Emacs versionning model.")

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

;;;
;;; PostgreSQL Connection Credentials and Session Settings
;;;
(defparameter *pgconn-host* "localhost")
(defparameter *pgconn-port* (parse-integer (getenv-default "PGPORT" "5432")))
(defparameter *pgconn-user* (uiop:getenv "USER"))
(defparameter *pgconn-pass* "pgpass")
(defparameter *pg-dbname* nil)
(defparameter *pg-settings* nil "An alist of GUC names and values.")

;;;
;;; MySQL Connection Credentials
;;;
(defparameter *myconn-host* "localhost")
(defparameter *myconn-port* 3306)
(defparameter *myconn-user* (uiop:getenv "USER"))
(defparameter *myconn-pass* nil)
(defparameter *my-dbname* nil)

;;;
;;; MSSQL Connection Credentials
;;;
(defparameter *msconn-host* "localhost")
(defparameter *msconn-port* 1433)
(defparameter *msconn-user* (uiop:getenv "USER"))
(defparameter *msconn-pass* nil)
(defparameter *ms-dbname* nil)

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

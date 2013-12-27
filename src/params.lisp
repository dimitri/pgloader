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
	   #:*copy-batch-rows*
	   #:*copy-batch-size*
	   #:*copy-batch-split*
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
	   #:*state*
	   #:*default-tmpdir*
	   #:init-params-from-environment
	   #:getenv-default))

(in-package :pgloader.params)

(defparameter *version-string* "3.0.96"
  "pgloader version strings, following Emacs versionning model.")

;; we can't use pgloader.utils:make-pgstate yet because params is compiled
;; first in the asd definition, we just make the symbol a special variable.
(defparameter *state* nil
  "State of the current loading.")

(defparameter *csv-path-root*
  (merge-pathnames "csv/" (user-homedir-pathname)))

(defparameter *root-dir*
  (make-pathname :directory "/tmp/pgloader/")
  "Top directory where to store all data logs and reject files.")

(defparameter *log-filename*
  (make-pathname :directory "/tmp/pgloader/" :name "pgloader" :type "log")
  "Main pgloader log file")

(defparameter *client-min-messages* :notice)
(defparameter *log-min-messages* :info)

;;;
;;; How to split batches in case of data loading errors.
;;;
(defparameter *copy-batch-rows* 25000
  "How many rows to batch per COPY transaction")

(defparameter *copy-batch-split* 5
  "Number of batches in which to split a batch with bad data")

;;;
;;; We need that to setup our default connection parameters
;;;
(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun getenv-default (name &optional default)
    "Return the value of the NAME variable as found in the environment, or
     DEFAULT if that variable isn't set"
    (or (uiop:getenv name) default)))

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
;;; Archive processing: downloads and unzip.
;;;
(defparameter *default-tmpdir*
  (let* ((tmpdir (uiop:getenv "TMPDIR"))
	 (tmpdir (or (and tmpdir (probe-file tmpdir)) "/tmp"))
	 (tmpdir (fad:pathname-as-directory tmpdir)))
    (fad:pathname-as-directory (merge-pathnames "pgloader" tmpdir)))
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

;;;
;;; pgloader parameters
;;;
;;; in a separate file to break circular dependencies

(defpackage #:pgloader.params
  (:use #:cl)
  (:export #:*csv-path-root*
	   #:*reject-path-root*
	   #:*log-filename*
	   #:*loader-kernel*
	   #:*copy-batch-size*
	   #:*copy-batch-split*
	   #:*pgconn-host*
	   #:*pgconn-port*
	   #:*pgconn-user*
	   #:*pgconn-pass*
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

(defparameter *log-filename*
  (make-pathname :directory "/tmp/pgloader/" :name "pgloader" :type "log"))

(defparameter *log-level* :notice)
(setq *log-level* :debug)

;;; package nicknames are only defined later, in package.lisp
(defparameter *loader-kernel* (lparallel:make-kernel 2)
  "lparallel kernel to use for loading data in parallel")

;;;
;;; How to split batches in case of data loading errors.
;;;
(defparameter *copy-batch-size* 25000
  "How many rows to per COPY transaction")

(defparameter *copy-batch-split* 5
  "Number of batches in which to split a batch with bad data")

;;;
;;; PostgreSQL Connection Credentials
;;;
(defparameter *pgconn-host* "localhost")
(defparameter *pgconn-port* 5432)
(defparameter *pgconn-user* (uiop:getenv "USER"))
(defparameter *pgconn-pass* "pgpass")

;;;
;;; MySQL Connection Credentials
;;;
(defparameter *myconn-host* "localhost")
(defparameter *myconn-port* 3306)
(defparameter *myconn-user* (uiop:getenv "USER"))
(defparameter *myconn-pass* nil)

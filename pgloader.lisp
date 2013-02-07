;;;; pgloader
;;;
;;; PostgreSQL data loading tool.
;;;

(in-package #:pgloader)

;;;
;;; Parameters you might want to change
;;;
(defparameter *loader-kernel* (lp:make-kernel 2)
  "lparallel kernel to use for loading data in parallel")

(defparameter *myconn-host* "myhost")
(defparameter *myconn-user* "myuser")
(defparameter *myconn-pass* "mypass")

(defparameter *state* (pgloader.utils:make-pgstate)
  "pgloader state, global stats and per-table stats")

;;;
;;; TODO: define a top level API
;;;

(defparameter *state* (pgloader.utils:make-pgstate)
  "State of the current loading.")

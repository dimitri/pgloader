;;;; pgloader
;;;
;;; PostgreSQL data loading tool.
;;;

(in-package #:pgloader)

;;;
;;; Internal Parameters. The one to change are in params.lisp
;;;
(defparameter *state* nil
  "State of the current loading.")

;;;
;;; TODO: define a top level API
;;;

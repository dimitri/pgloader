;;;; package.lisp

(defpackage #:pgloader.pgsql
  (:use #:cl)
  (:export #:truncate-table
	   #:copy-from-file
	   #:copy-from-queue
	   #:list-databases
	   #:list-tables
	   #:get-date-columns
	   #:format-row))

(defpackage #:pgloader
  (:use #:cl)
  (:import-from #:pgloader.pgsql
		#:copy-from-file
		#:list-databases
		#:list-tables)
  (:export #:copy-from-file
	   #:list-databases
	   #:list-tables))

(in-package #:pgloader)

;;;
;;; Some package names are a little too long to my taste and don't ship with
;;; nicknames, so use `rename-package' here to give them some new nicknames.
;;;
(loop
   for (package . nicknames)
   in '((lparallel lp)
	(lparallel.queue lq)
	(simple-date date)
	(split-sequence sq))
   do (rename-package package package nicknames))

;;;
;;; and recompile. Now you can pre-allocate the queue by passing a size to
;;; MAKE-QUEUE. (You could pass a number before too, but it was ignored.)
;;;
(pushnew :lparallel.with-vector-queue *features*)

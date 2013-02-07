;;;; package.lisp
;;;
;;; To avoid circular files dependencies, define all the packages here
;;;

(defpackage #:pgloader.utils
  (:use #:cl)
  (:export #:report-header
	   #:report-table-name
	   #:report-results
	   #:report-footer
	   #:format-interval
	   #:timing
	   #:make-pgstate
	   #:pgstate-get-table
	   #:pgstate-add-table
	   #:pgstate-setf
	   #:pgstate-incf
	   #:report-pgtable-stats
	   #:report-pgstate-stats))

(defpackage #:pgloader.queue
  (:use #:cl)
  (:export #:map-pop-queue
	   #:map-push-queue))

(defpackage #:pgloader.csv
  (:use #:cl)
  (:export #:*csv-path-root*
	   #:get-pathname))

(defpackage #:pgloader.mysql
  (:use #:cl)
  (:import-from #:pgloader
		#:*loader-kernel*
		#:*myconn-host*
		#:*myconn-user*
		#:*myconn-pass*)
  (:import-from #:pgloader.utils
		#:report-header
		#:report-table-name
		#:report-results
		#:report-footer
		#:format-interval
		#:timing
		#:make-pgstate
		#:pgstate-get-table
		#:pgstate-add-table
		#:pgstate-setf
		#:pgstate-incf
		#:report-pgtable-stats
		#:report-pgstate-stats)
  (:import-from #:pgloader
		#:*state*)
  (:export #:map-rows
	   #:copy-from
	   #:list-databases
	   #:list-tables
	   #:export-all-tables
	   #:export-import-database
	   #:stream-mysql-table-in-pgsql
	   #:stream-database-tables))

(defpackage #:pgloader.pgsql
  (:use #:cl)
  (:import-from #:pgloader.utils
		#:report-header
		#:report-table-name
		#:report-results
		#:report-footer
		#:format-interval
		#:timing
		#:make-pgstate
		#:pgstate-get-table
		#:pgstate-add-table
		#:pgstate-setf
		#:pgstate-incf
		#:report-pgtable-stats
		#:report-pgstate-stats)
  (:import-from #:pgloader
		#:*state*)
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
  (:export #:*state*
	   #:copy-from-file
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

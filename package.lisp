;;;; package.lisp
;;;
;;; To avoid circular files dependencies, define all the packages here
;;;

(defpackage #:pgloader.utils
  (:use #:cl)
  (:import-from #:cl-log
		#:defcategory
		#:log-manager
		#:start-messenger
		#:log-message
		#:ring-messenger
		#:text-file-messenger
		#:formatted-message)
  (:import-from #:pgloader.params
		#:*reject-path-root*
		#:*log-filename*
		#:*log-level*
		#:*state*)
  (:export #:log-message
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
	   #:pgstate-decf
	   #:pgtable-reject-data
	   #:pgtable-reject-logs
	   #:report-pgtable-stats
	   #:report-pgstate-stats))

(defpackage #:pgloader.queue
  (:use #:cl)
  (:export #:map-pop-queue
	   #:map-push-queue))

(defpackage #:pgloader.csv
  (:use #:cl)
  (:import-from #:pgloader.params
		#:*csv-path-root*
		#:*loader-kernel*
		#:*state*)
  (:import-from #:pgloader.utils
		#:log-message
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
		#:pgstate-decf
		#:report-pgtable-stats
		#:report-pgstate-stats)
  (:export #:*csv-path-root*
	   #:get-pathname
	   #:copy-to-queue
	   #:copy-from-file
	   #:import-database
	   #:guess-csv-params
	   #:guess-all-csv-params))

(defpackage #:pgloader.mysql
  (:use #:cl)
  (:import-from #:pgloader.params
		#:*csv-path-root*
		#:*reject-path-root*
		#:*loader-kernel*
		#:*myconn-host*
		#:*myconn-user*
		#:*myconn-pass*
		#:*state*)
  (:import-from #:pgloader.utils
		#:log-message
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
		#:pgstate-decf
		#:report-pgtable-stats
		#:report-pgstate-stats)
  (:export #:map-rows
	   #:copy-to
	   #:list-databases
	   #:list-tables
	   #:export-database
	   #:export-import-database
	   #:stream-table
	   #:stream-database))

(defpackage #:pgloader.pgsql
  (:use #:cl)
  (:import-from #:pgloader.params
		#:*csv-path-root*
		#:*reject-path-root*
		#:*loader-kernel*
		#:*state*)
  (:import-from #:pgloader.utils
		#:log-message
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
		#:pgstate-decf
		#:pgtable-reject-data
		#:pgtable-reject-logs
		#:report-pgtable-stats
		#:report-pgstate-stats)
  (:export #:truncate-table
	   #:copy-from-file
	   #:copy-from-queue
	   #:list-databases
	   #:list-tables
	   #:list-tables-cols
	   #:reset-all-sequences
	   #:execute
	   #:get-date-columns
	   #:format-row))

(defpackage #:pgloader
  (:use #:cl)
  (:import-from #:pgloader.params
		#:*csv-path-root*
		#:*reject-path-root*
		#:*loader-kernel*
		#:*myconn-host*
		#:*myconn-user*
		#:*myconn-pass*
		#:*state*)
  (:import-from #:pgloader.pgsql
		#:copy-from-file
		#:list-databases
		#:list-tables)
  (:export #:*state*
	   #:*csv-path-root*
	   #:*reject-path-root*
	   #:*loader-kernel*
	   #:*myconn-host*
	   #:*myconn-user*
	   #:*myconn-pass*
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

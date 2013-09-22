;;;; package.lisp
;;;
;;; To avoid circular files dependencies, define all the packages here
;;;

(defpackage #:pgloader.utils
  (:use #:cl #:pgloader.params)
  (:import-from #:cl-log
		#:defcategory
		#:log-manager
		#:start-messenger
		#:log-message
		#:ring-messenger
		#:text-file-messenger
		#:formatted-message)
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
	   #:report-pgstate-stats
	   #:slurp-file-into-string
	   #:camelCase-to-colname))

(defpackage #:pgloader.transforms
  (:use #:cl))

(defpackage #:pgloader.parser
  (:use #:cl #:esrap #:pgloader.params)
  (:export #:parse-command
	   #:run-command))

(defpackage #:pgloader.queue
  (:use #:cl)
  (:export #:map-pop-queue
	   #:map-push-queue))

(defpackage #:pgloader.csv
  (:use #:cl #:pgloader.params)
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

(defpackage #:pgloader.db3
  (:use #:cl #:pgloader.params)
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
	   #:copy-to-queue
	   #:stream-file))

(defpackage #:pgloader.archive
  (:use #:cl #:pgloader.params #:pgloader.csv)
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
		#:report-pgstate-stats
		#:camelCase-to-colname)
  (:export #:import-csv-from-zip))

(defpackage #:pgloader.syslog
  (:use #:cl #:pgloader.params)
  (:import-from #:pgloader.utils
		#:log-message)
  (:export #:stream-messages
	   #:start-syslog-server
	   #:send-message))

(defpackage #:pgloader.mysql
  (:use #:cl #:pgloader.params)
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
  (:export #:*cast-rules*
	   #:*default-cast-rules*
	   #:map-rows
	   #:copy-to
	   #:list-databases
	   #:list-tables
	   #:export-database
	   #:export-import-database
	   #:stream-table
	   #:stream-database))

(defpackage #:pgloader.pgsql
  (:use #:cl #:pgloader.params)
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
  (:use #:cl #:pgloader.params)
  (:import-from #:pgloader.pgsql
		#:copy-from-file
		#:list-databases
		#:list-tables)
  (:import-from #:pgloader.utils
		#:slurp-file-into-string)
  (:import-from #:pgloader.parser
		#:run-command
		#:parse-command)
  (:export #:*state*
	   #:*csv-path-root*
	   #:*reject-path-root*
	   #:*myconn-host*
	   #:*myconn-port*
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

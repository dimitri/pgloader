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
		#:text-stream-messenger
		#:formatted-message)
  (:export #:start-logger
	   #:stop-logger
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
	   #:report-pgstate-stats
	   #:report-summary
	   #:report-full-summary
	   #:with-stats-collection
	   #:slurp-file-into-string
	   #:camelCase-to-colname
	   #:make-kernel))

(defpackage #:pgloader.queue
  (:use #:cl)
  (:export #:map-pop-queue
	   #:map-push-queue))

(defpackage #:pgloader.pgsql
  (:use #:cl #:pgloader.params #:pgloader.utils)
  (:export #:with-pgsql-transaction
	   #:with-pgsql-connection
	   #:pgsql-execute
	   #:pgsql-execute-with-timing
	   #:truncate-table
	   #:copy-from-file
	   #:copy-from-queue
	   #:list-databases
	   #:list-tables
	   #:list-columns
	   #:list-tables-cols
	   #:list-tables-and-fkeys
	   #:list-reserved-keywords
	   #:list-table-oids
	   #:reset-all-sequences
	   #:get-date-columns
	   #:format-row
	   #:apply-identifier-case
	   #:create-tables
	   #:format-pgsql-column
	   #:format-extra-type
	   #:make-pgsql-fkey
	   #:format-pgsql-create-fkey
	   #:format-pgsql-drop-fkey
	   #:make-pgsql-index
	   #:index-table-name
	   #:format-pgsql-create-index
	   #:create-indexes-in-kernel))

(defpackage #:pgloader.ini
  (:use #:cl #:pgloader.params #:pgloader.utils)
  (:import-from #:pgloader.pgsql
		#:list-columns
		#:with-pgsql-transaction
		#:pgsql-execute)
  (:export #:read-ini-file
	   #:parse-ini-file
	   #:write-command-to-string
	   #:convert-ini-into-commands
	   #:convert-ini-into-files))

(defpackage #:pgloader.parser
  (:use #:cl #:esrap #:pgloader.params #:pgloader.utils)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute)
  (:export #:parse-commands
	   #:run-commands
	   #:with-database-uri))


;;
;; Specific source handling
;;
(defpackage #:pgloader.sources
  (:use #:cl #:pgloader.params #:pgloader.utils)
  (:export #:copy
	   #:source-db
	   #:target-db
	   #:source
	   #:target
	   #:fields
	   #:columns
	   #:transforms
	   #:map-rows
	   #:copy-from
	   #:copy-to-queue
	   #:copy-to
	   #:copy-database
	   #:filter-column-list
	   #:get-pathname
	   #:get-absolute-pathname
	   #:project-fields
	   #:reformat-then-process))

(defpackage #:pgloader.csv
  (:use #:cl #:pgloader.params #:pgloader.utils #:pgloader.sources)
  (:export #:*csv-path-root*
	   #:get-pathname
	   #:copy-csv
	   #:copy-to-queue
	   #:copy-from
	   #:import-database
	   #:guess-csv-params
	   #:guess-all-csv-params))

(defpackage #:pgloader.fixed
  (:use #:cl #:pgloader.params #:pgloader.utils #:pgloader.sources)
  (:export #:copy-fixed
	   #:copy-to-queue
	   #:copy-from))

(defpackage #:pgloader.db3
  (:use #:cl #:pgloader.params #:pgloader.utils #:pgloader.sources)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:apply-identifier-case
		#:create-tables
		#:format-pgsql-column)
  (:export #:copy-db3
	   #:map-rows
	   #:copy-to
	   #:copy-to-queue
	   #:stream-file))

(defpackage #:pgloader.mysql
  (:use #:cl #:pgloader.params #:pgloader.utils #:pgloader.sources)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:apply-identifier-case
		#:list-tables-and-fkeys
		#:list-table-oids
		#:create-tables
		#:format-pgsql-column
		#:format-extra-type
		#:make-pgsql-fkey
		#:format-pgsql-create-fkey
		#:format-pgsql-drop-fkey
		#:make-pgsql-index
		#:format-pgsql-create-index
		#:create-indexes-in-kernel)
  (:export #:copy-mysql
	   #:*cast-rules*
	   #:*default-cast-rules*
           #:with-mysql-connection
	   #:map-rows
	   #:copy-to
	   #:copy-from
	   #:copy-database
	   #:list-databases
	   #:list-tables
	   #:export-database
	   #:export-import-database))

(defpackage #:pgloader.sqlite
  (:use #:cl #:pgloader.params #:pgloader.utils #:pgloader.sources)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:apply-identifier-case
		#:create-tables
		#:format-pgsql-column
		#:make-pgsql-index
		#:index-table-name
		#:format-pgsql-create-index
		#:create-indexes-in-kernel)
  (:export #:copy-sqlite
	   #:map-rows
	   #:copy-to
	   #:copy-from
	   #:copy-database
	   #:list-tables))

(defpackage #:pgloader.syslog
  (:use #:cl #:pgloader.params #:pgloader.utils)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute)
  (:export #:stream-messages
	   #:start-syslog-server
	   #:send-message))


;;
;; Not really a source, more a util package to deal with http and zip
;;
(defpackage #:pgloader.archive
  (:use #:cl #:pgloader.params #:pgloader.utils #:pgloader.csv)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute)
  (:export #:*default-tmpdir*
	   #:http-fetch-file
	   #:expand-archive
	   #:get-matching-filenames
	   #:import-csv-from-zip))



;;
;; Main package
;;
(defpackage #:pgloader
  (:use #:cl #:pgloader.params #:pgloader.utils)
  (:import-from #:pgloader.pgsql
		#:copy-from-file
		#:list-databases
		#:list-tables)
  (:import-from #:pgloader.parser
		#:run-commands
		#:parse-commands
		#:with-database-uri)
  (:export #:*version-string*
	   #:*state*
	   #:*csv-path-root*
	   #:*root-dir*
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
	   #:run-commands
	   #:parse-commands
	   #:with-database-uri
	   #:slurp-file-into-string
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
	(split-sequence sq)
	(com.informatimago.clext.character-sets charsets)
	(py-configparser ini))
   do (rename-package package package nicknames))

;;;
;;; and recompile. Now you can pre-allocate the queue by passing a size to
;;; MAKE-QUEUE. (You could pass a number before too, but it was ignored.)
;;;
(pushnew :lparallel.with-vector-queue *features*)

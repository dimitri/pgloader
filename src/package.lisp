;;;; package.lisp
;;;
;;; To avoid circular files dependencies, define all the packages here
;;;
(defpackage #:pgloader.transforms
  (:use #:cl)
  (:export #:precision
           #:scale
           #:intern-symbol))

(defpackage #:pgloader.logs
  (:use #:cl #:pgloader.params)
  (:import-from #:cl-log
		#:defcategory
		#:log-manager
		#:start-messenger
		#:ring-messenger
		#:text-file-messenger
		#:text-stream-messenger
		#:formatted-message)
  (:export #:*log-messengers*
           #:start-logger
           #:stop-logger))

(defpackage #:pgloader.monitor
  (:use #:cl #:pgloader.params)
  (:export #:with-monitor
           #:*monitoring-queue*
           #:log-message
           #:send-event
           #:start-monitor
           #:stop-monitor))

(defpackage #:pgloader.utils
  (:use #:cl #:pgloader.params)
  (:import-from #:alexandria #:read-file-into-string)
  (:import-from #:pgloader.monitor
                #:with-monitor
                #:*monitoring-queue*
                #:log-message)
  (:export #:with-monitor
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
	   #:camelCase-to-colname
           #:unquote
           #:make-kernel
           #:list-encodings-and-aliases
           #:show-encodings
           #:make-external-format))


;;;
;;; PostgreSQL COPY support, and generic sources API.
;;;
(defpackage #:pgloader.parse-date
  (:use #:cl #:esrap)
  (:export #:parse-date-string
           #:parse-date-format))

(defpackage #:pgloader.sources
  (:use #:cl #:pgloader.params #:pgloader.utils)
  (:import-from #:pgloader.transforms #:precision #:scale)
  (:import-from #:pgloader.parse-date
                #:parse-date-string
                #:parse-date-format)
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

           ;; conditions, error handling
           #:connection-error

           ;; file based utils for CSV, fixed etc
	   #:filter-column-list
           #:with-open-file-or-stream
	   #:get-pathname
	   #:get-absolute-pathname
	   #:project-fields
	   #:reformat-then-process

           ;; database cast machinery
           #:*default-cast-rules*
           #:*cast-rules*
           #:cast))

(defpackage #:pgloader.pgsql
  (:use #:cl #:pgloader.params #:pgloader.utils)
  (:import-from #:pgloader.sources
                #:connection-error)
  (:export #:with-pgsql-transaction
	   #:with-pgsql-connection
	   #:pgsql-execute
	   #:pgsql-execute-with-timing
	   #:truncate-tables
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
           #:format-vector-row
	   #:apply-identifier-case
	   #:create-tables
	   #:format-pgsql-column
	   #:format-extra-type
	   #:make-pgsql-fkey
	   #:format-pgsql-create-fkey
	   #:format-pgsql-drop-fkey
           #:drop-pgsql-fkeys
           #:create-pgsql-fkeys
	   #:make-pgsql-index
	   #:index-table-name
	   #:format-pgsql-create-index
	   #:create-indexes-in-kernel
           #:set-table-oids
           #:reset-sequences))

(defpackage #:pgloader.queue
  (:use #:cl #:pgloader.params)
  (:import-from #:pgloader.monitor
                #:log-message)
  (:import-from #:pgloader.pgsql
                #:format-vector-row)
  (:import-from #:pgloader.sources
                #:map-rows
                #:transforms)
  (:export #:map-push-queue))


;;;
;;; Other utilities
;;;
(defpackage #:pgloader.ini
  (:use #:cl #:pgloader.params #:pgloader.utils)
  (:import-from #:alexandria #:read-file-into-string)
  (:import-from #:pgloader.pgsql
		#:list-columns
		#:with-pgsql-transaction
		#:pgsql-execute)
  (:export #:read-ini-file
	   #:parse-ini-file
	   #:write-command-to-string
	   #:convert-ini-into-commands
	   #:convert-ini-into-files))

(defpackage #:pgloader.sql
  (:use #:cl)
  (:export #:read-queries))


;;
;; Specific source handling
;;
(defpackage #:pgloader.csv
  (:use #:cl
        #:pgloader.params #:pgloader.utils
        #:pgloader.sources #:pgloader.queue)
  (:export #:*csv-path-root*
	   #:get-pathname
	   #:copy-csv
	   #:copy-to-queue
	   #:copy-from
	   #:import-database
	   #:guess-csv-params
	   #:guess-all-csv-params))

(defpackage #:pgloader.fixed
  (:use #:cl
        #:pgloader.params #:pgloader.utils
        #:pgloader.sources #:pgloader.queue)
  (:export #:copy-fixed
	   #:copy-to-queue
	   #:copy-from))

(defpackage #:pgloader.ixf
  (:use #:cl
        #:pgloader.params #:pgloader.utils
        #:pgloader.sources #:pgloader.queue)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:apply-identifier-case
		#:create-tables
		#:format-pgsql-column
                #:format-vector-row)
  (:export #:copy-ixf
	   #:map-rows
	   #:copy-to-queue
	   #:copy-from))

(defpackage #:pgloader.db3
  (:use #:cl
        #:pgloader.params #:pgloader.utils
        #:pgloader.sources #:pgloader.queue)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:apply-identifier-case
		#:create-tables
		#:format-pgsql-column
                #:format-vector-row)
  (:export #:copy-db3
	   #:map-rows
	   #:copy-to
	   #:copy-to-queue
	   #:copy-from))

(defpackage #:pgloader.mysql
  (:use #:cl
        #:pgloader.params #:pgloader.utils
        #:pgloader.sources #:pgloader.queue)
  (:import-from #:pgloader.transforms #:precision #:scale)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:apply-identifier-case
		#:list-tables-and-fkeys
		#:list-table-oids
		#:create-tables
                #:truncate-tables
		#:format-pgsql-column
		#:format-extra-type
		#:make-pgsql-fkey
		#:format-pgsql-create-fkey
		#:format-pgsql-drop-fkey
                #:drop-pgsql-fkeys
                #:create-pgsql-fkeys
		#:make-pgsql-index
		#:format-pgsql-create-index
		#:create-indexes-in-kernel
                #:set-table-oids
                #:format-vector-row
                #:reset-sequences)
  (:export #:copy-mysql
	   #:*mysql-default-cast-rules*
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
  (:use #:cl
        #:pgloader.params #:pgloader.utils
        #:pgloader.sources #:pgloader.queue)
  (:import-from #:pgloader.transforms #:precision #:scale)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:apply-identifier-case
		#:create-tables
                #:truncate-tables
		#:format-pgsql-column
		#:make-pgsql-index
		#:index-table-name
		#:format-pgsql-create-index
		#:create-indexes-in-kernel
                #:set-table-oids
                #:reset-sequences)
  (:export #:copy-sqlite
           #:*sqlite-default-cast-rules*
	   #:map-rows
	   #:copy-to
	   #:copy-from
	   #:copy-database
	   #:list-tables))

(defpackage #:pgloader.mssql
  (:use #:cl
        #:pgloader.params #:pgloader.utils
        #:pgloader.sources #:pgloader.queue)
  (:import-from #:pgloader.transforms #:precision #:scale)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:apply-identifier-case
		#:list-tables-and-fkeys
		#:list-table-oids
		#:create-tables
                #:truncate-tables
		#:format-pgsql-column
		#:format-extra-type
		#:make-pgsql-fkey
		#:format-pgsql-create-fkey
		#:format-pgsql-drop-fkey
                #:drop-pgsql-fkeys
                #:create-pgsql-fkeys
		#:make-pgsql-index
		#:format-pgsql-create-index
		#:create-indexes-in-kernel
                #:set-table-oids
                #:format-vector-row
                #:reset-sequences)
  (:export #:copy-mssql
           #:*mssql-default-cast-rules*
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
  (:use #:cl #:pgloader.params)
  (:import-from #:pgloader.monitor
                #:log-message)
  (:export #:http-fetch-file
	   #:expand-archive
	   #:get-matching-filenames))


;;;
;;; The Command Parser
;;;
(defpackage #:pgloader.parser
  (:use #:cl #:esrap #:metabang.bind
        #:pgloader.params #:pgloader.utils #:pgloader.sql)
  (:import-from #:alexandria #:read-file-into-string)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute)
  (:import-from #:pgloader.sources
                #:*default-cast-rules*
                #:*cast-rules*)
  (:import-from #:pgloader.mysql  #:*mysql-default-cast-rules*)
  (:import-from #:pgloader.mssql  #:*mssql-default-cast-rules*)
  (:import-from #:pgloader.sqlite #:*sqlite-default-cast-rules*)
  (:export #:parse-commands
	   #:run-commands

           ;; tools to enable complete CLI parsing in main.lisp
           #:process-relative-pathnames
	   #:parse-source-string
	   #:parse-source-string-for-type
           #:parse-target-string
           #:parse-cli-options
           #:parse-cli-gucs
           #:parse-cli-type
           #:parse-cli-encoding
           #:parse-cli-fields
           #:parse-cli-casts
           #:parse-sql-file

           ;; functions to generate lisp code from parameters
           #:lisp-code-for-loading-from-mysql
           #:lisp-code-for-loading-from-csv
           #:lisp-code-for-loading-from-fixed
           #:lisp-code-for-loading-from-dbf
           #:lisp-code-for-loading-from-ixf
           #:lisp-code-for-loading-from-sqlite
           #:lisp-code-for-loading-from-mssql))


;;
;; Main package
;;
(defpackage #:pgloader
  (:use #:cl #:pgloader.params #:pgloader.utils #:pgloader.parser)
  (:import-from #:pgloader.pgsql
		#:copy-from-file
		#:list-databases
		#:list-tables)
  (:import-from #:pgloader.sources
                #:connection-error)
  (:export #:*version-string*
	   #:*state*
	   #:*csv-path-root*
	   #:*root-dir*
	   #:*pgconn*
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
	(py-configparser ini))
   do (rename-package package package nicknames))

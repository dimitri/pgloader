;;;; package.lisp
;;;
;;; To avoid circular files dependencies, define all the packages here
;;;
(eval-when (:compile-toplevel :load-toplevel :execute)
  (defun cl-user::export-inherited-symbols (source target)
    (let ((pkg-source (find-package (string-upcase source)))
          (pkg-target (find-package (string-upcase target))))
      (do-external-symbols (s pkg-source)
        (export s pkg-target)))))

(defpackage #:pgloader.transforms
  (:use #:cl)
  (:export #:precision
           #:scale
           #:intern-symbol
           #:typemod-expr-to-function))

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

(defpackage #:pgloader.quoting
  (:use #:cl #:pgloader.params)
  (:export #:apply-identifier-case))

(defpackage #:pgloader.catalog
  (:use #:cl #:pgloader.params #:pgloader.quoting)
  (:export #:format-create-sql
           #:format-drop-sql
           #:format-default-value

           #:catalog
           #:schema
           #:table
           #:sqltype
           #:column
           #:index
           #:fkey
           #:trigger
           #:procedure

           #:cast                       ; generic function for sources

           #:apply-identifier-case

           #:make-catalog
           #:make-schema
           #:make-table
           #:create-table
           #:make-view
           #:make-sqltype
           #:make-column
           #:make-index
           #:make-fkey
           #:make-trigger
           #:make-procedure

           #:catalog-name
           #:catalog-schema-list

           #:schema-name
           #:schema-catalog
           #:schema-source-name
           #:schema-table-list
           #:schema-view-list

           #:table-name
           #:table-source-name
           #:table-schema
           #:table-oid
           #:table-comment
           #:table-field-list
           #:table-column-list
           #:table-index-list
           #:table-fkey-list
           #:table-trigger-list

           #:sqltype-name
           #:sqltype-type
           #:sqltype-source-def
           #:sqltype-extra

           #:column-name
           #:column-type-name
           #:column-type-mod
           #:column-nullable
           #:column-default
           #:column-comment
           #:column-transform
           #:column-extra

           #:index-name
           #:index-oid
           #:index-schema
           #:index-table
           #:index-primary
           #:index-unique
           #:index-columns
           #:index-sql
           #:index-conname
           #:index-condef
           #:index-filter
           #:index-fk-deps

           #:fkey-name
           #:fkey-oid
           #:fkey-foreign-table
           #:fkey-foreign-columns
           #:fkey-table
           #:fkey-columns
           #:fkey-condef
           #:fkey-update-rule
           #:fkey-delete-rule
           #:fkey-match-rule
           #:fkey-deferrable
           #:fkey-initially-deferred

           #:trigger-name
           #:trigger-table
           #:trigger-action
           #:trigger-procedure-name
           #:trigger-procedure

           #:procedure-name
           #:procedure-returns
           #:procedure-language
           #:procedure-body

           #:table-list
           #:view-list
           #:add-schema
           #:find-schema
           #:maybe-add-schema
           #:add-table
           #:find-table
           #:maybe-add-table
           #:add-view
           #:find-view
           #:maybe-add-view
           #:add-field
           #:add-column
           #:add-index
           #:find-index
           #:maybe-add-index
           #:add-fkey
           #:find-fkey
           #:maybe-add-fkey
           #:count-tables
           #:count-views
           #:count-indexes
           #:count-fkeys
           #:max-indexes-per-table

           #:push-to-end
           #:with-schema

           #:alter-table
           #:alter-schema
           #:string-match-rule
           #:make-string-match-rule
           #:string-match-rule-target
           #:regex-match-rule
           #:make-regex-match-rule
           #:regex-match-rule-target
           #:matches
           #:match-rule
           #:make-match-rule
           #:match-rule-rule
           #:match-rule-schema
           #:match-rule-action
           #:match-rule-args

           #:format-table-name))

(defpackage #:pgloader.state
  (:use #:cl #:pgloader.params #:pgloader.catalog)
  (:export #:make-pgstate
           #:pgstate-tabnames
           #:pgstate-tables
           #:pgstate-read
           #:pgstate-rows
           #:pgstate-errs
           #:pgstate-secs

           #:pgstate-get-label
           #:pgstate-new-label
           #:pgstate-setf
           #:pgstate-incf
           #:pgstate-decf

           #:pgtable-initialize-reject-files
           #:pgtable-reject-data
           #:pgtable-reject-logs
           #:report-pgtable-stats
           #:report-pgstate-stats

           ;; report
           #:report-header
	   #:report-table-name
	   #:report-results
	   #:report-footer
	   #:report-summary
	   #:report-full-summary))

(defpackage #:pgloader.monitor
  (:use #:cl #:pgloader.params #:pgloader.state)
  (:export #:with-monitor
           #:*monitoring-queue*
           #:log-message
           #:new-label
           #:update-stats
           #:process-bad-row
           #:flush-summary
           #:with-stats-collection
           #:send-event
           #:start-monitor
           #:stop-monitor
           #:elapsed-time-since
           #:timing))

(defpackage #:pgloader.utils
  (:use #:cl
        #:pgloader.params #:pgloader.catalog #:pgloader.monitor #:pgloader.state)
  (:import-from #:alexandria
                #:appendf
                #:read-file-into-string)

  (:export ;; bits from alexandria
           #:appendf
           #:read-file-into-string

           ;; utils
	   #:format-interval
	   #:camelCase-to-colname
           #:unquote
           #:expand-user-homedir-pathname

           ;; threads
           #:make-kernel

           ;; charsets
           #:list-encodings-and-aliases
           #:show-encodings
           #:make-external-format

           ;; quoting
           #:apply-identifier-case))

(cl-user::export-inherited-symbols "pgloader.catalog" "pgloader.utils")
(cl-user::export-inherited-symbols "pgloader.monitor" "pgloader.utils")
(cl-user::export-inherited-symbols "pgloader.state"   "pgloader.utils")

(defpackage #:pgloader.batch
  (:use #:cl #:pgloader.params #:pgloader.monitor)
  (:export #:make-batch
           #:batch-row
           #:finish-batch
           #:push-end-of-data-message))


;;
;; Not really a source, more a util package to deal with http and zip
;;
(defpackage #:pgloader.archive
  (:use #:cl #:pgloader.params)
  (:import-from #:pgloader.monitor
                #:log-message)
  (:export #:*supporter-archive-types*
           #:archivep
           #:http-fetch-file
	   #:expand-archive
	   #:get-matching-filenames))


;;;
;;; PostgreSQL COPY support, and generic sources API.
;;;
(defpackage #:pgloader.parse-date
  (:use #:cl #:esrap)
  (:export #:parse-date-string
           #:parse-date-format))

(defpackage #:pgloader.connection
  (:use #:cl #:pgloader.archive)
  (:import-from #:pgloader.monitor
                #:log-message)
  (:export #:connection
           #:open-connection
           #:close-connection
           #:clone-connection
           #:fd-connection
           #:db-connection
           #:connection-error
           #:fd-connection-error
           #:db-connection-error
           #:with-connection
           #:query
           #:check-connection

           ;; also export slot names
           #:type
           #:handle
           #:uri
           #:arch
           #:path

           ;; file based connections API for HTTP and Archives support
           #:fetch-file
           #:expand

           ;; connection classes accessors
           #:conn-type
           #:conn-handle
           #:db-conn
           #:fd-path
           #:db-name
           #:db-host
           #:db-port
           #:db-user
           #:db-pass))

(defpackage #:pgloader.pgsql
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.catalog)
  (:export #:pgsql-connection
           #:pgconn-use-ssl
           #:pgconn-table-name
           #:with-pgsql-transaction
	   #:with-pgsql-connection
	   #:pgsql-execute
	   #:pgsql-execute-with-timing
	   #:pgsql-connect-and-execute-with-timing

           ;; postgresql schema facilities
	   #:truncate-tables
	   #:copy-from-file
	   #:copy-from-queue
           #:set-table-oids

           #:create-sqltypes
	   #:create-schemas
	   #:create-tables
	   #:create-views
           #:drop-pgsql-fkeys
           #:create-pgsql-fkeys
           #:create-triggers

           #:translate-index-filter
           #:process-index-definitions

           #:fetch-pgsql-catalog
           #:merge-catalogs

	   #:create-indexes-in-kernel
           #:drop-indexes
           #:maybe-drop-indexes
           #:create-indexes-again
           #:reset-sequences
           #:comment-on-tables-and-columns

           ;; index filter rewriting support
           #:translate-index-filter
           #:process-index-definitions

           ;; postgresql introspection queries
	   #:list-all-columns
	   #:list-all-indexes
	   #:list-all-fkeys
	   #:list-missing-fk-deps
	   #:list-schemas
	   #:list-table-oids

           ;; postgresql identifiers
	   #:list-reserved-keywords

           ;; postgresql user provided gucs
           #:sanitize-user-gucs

           ;; postgresql data format
	   #:get-date-columns
           #:format-vector-row))

(defpackage #:pgloader.sources
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.pgsql #:pgloader.batch)
  (:import-from #:pgloader.transforms
                #:precision
                #:scale
                #:intern-symbol
                #:typemod-expr-to-function)
  (:import-from #:pgloader.parse-date
                #:parse-date-string
                #:parse-date-format)
  (:export #:copy
           #:md-copy
           #:db-copy

           ;; accessors
	   #:source-db
	   #:target-db
	   #:source
	   #:target
	   #:fields
	   #:columns
	   #:transforms
           #:encoding
           #:skip-lines
           #:header

           ;; main protocol/api
	   #:map-rows
           #:copy-column-list
           #:queue-raw-data
           #:format-data-to-copy
	   #:copy-from
	   #:copy-to
	   #:copy-database

           ;; md-copy protocol/api
           #:parse-header
           #:process-rows

           ;; the md-connection facilities
           #:md-connection
           #:md-spec
           #:md-strm
           #:expand-spec
           #:clone-copy-for

           ;; the db-methods
           #:fetch-metadata
           #:prepare-pgsql-database
           #:cleanup
           #:instanciate-table-copy-object
           #:complete-pgsql-database
           #:end-kernels

           ;; file based utils for csv, fixed etc
           #:with-open-file-or-stream
	   #:get-pathname
	   #:project-fields
	   #:reformat-then-process

           ;; database cast machinery
           #:*default-cast-rules*
           #:*cast-rules*
           #:apply-casting-rules
           #:format-pgsql-type))


;;;
;;; other utilities
;;;
(defpackage #:pgloader.ini
  (:use #:cl #:pgloader.params #:pgloader.utils #:pgloader.connection)
  (:import-from #:alexandria #:read-file-into-string)
  (:import-from #:pgloader.pgsql
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
;; specific source handling
;;
(defpackage #:pgloader.csv
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.sources)
  (:import-from #:pgloader.pgsql
                #:maybe-drop-indexes
                #:create-indexes-again)
  (:export #:csv-connection
           #:specs
           #:csv-specs
	   #:get-pathname
	   #:copy-csv
	   #:copy-from
	   #:import-database
	   #:guess-csv-params
	   #:guess-all-csv-params))

(defpackage #:pgloader.fixed
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.sources)
  (:import-from #:pgloader.csv
                #:csv-connection
                #:specs
                #:csv-specs)
  (:import-from #:pgloader.pgsql
                #:maybe-drop-indexes
                #:create-indexes-again)
  (:export #:fixed-connection
           #:copy-fixed
	   #:copy-from))

(defpackage #:pgloader.copy
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.sources)
  (:import-from #:pgloader.csv
                #:csv-connection
                #:specs
                #:csv-specs)
  (:import-from #:pgloader.pgsql
                #:maybe-drop-indexes
                #:create-indexes-again)
  (:export #:copy-connection
           #:copy-copy
	   #:copy-from))

(defpackage #:pgloader.ixf
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.sources)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:create-tables
                #:format-vector-row)
  (:export #:ixf-connection
           #:copy-ixf
	   #:map-rows
	   #:copy-from))

(defpackage #:pgloader.db3
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.sources)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:create-tables
                #:format-vector-row)
  (:export #:dbf-connection
           #:copy-db3
	   #:map-rows
	   #:copy-to
	   #:copy-from))

(defpackage #:pgloader.mysql
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.sources)
  (:import-from #:pgloader.transforms #:precision #:scale)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-connection
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:list-table-oids
		#:create-tables
		#:create-views
                #:truncate-tables
                #:drop-pgsql-fkeys
                #:create-pgsql-fkeys
		#:create-indexes-in-kernel
                #:format-vector-row
                #:reset-sequences
                #:comment-on-tables-and-columns)
  (:export #:mysql-connection
           #:copy-mysql
           #:*decoding-as*
	   #:*mysql-default-cast-rules*
           #:with-mysql-connection
	   #:map-rows
	   #:copy-to
	   #:copy-from
	   #:copy-database
	   #:list-databases
	   #:export-database
	   #:export-import-database))

(defpackage #:pgloader.sqlite
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.sources)
  (:import-from #:pgloader.transforms #:precision #:scale)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:create-tables
                #:truncate-tables
		#:create-indexes-in-kernel
                #:reset-sequences
                #:comment-on-tables-and-columns)
  (:export #:sqlite-connection
           #:copy-sqlite
           #:*sqlite-default-cast-rules*
	   #:map-rows
	   #:copy-to
	   #:copy-from
	   #:copy-database))

(defpackage #:pgloader.mssql
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.sources)
  (:import-from #:pgloader.transforms #:precision #:scale)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-connection
		#:with-pgsql-transaction
		#:pgsql-execute
		#:pgsql-execute-with-timing
		#:pgsql-connect-and-execute-with-timing
		#:list-table-oids
		#:create-tables
		#:create-views
                #:drop-pgsql-fkeys
                #:create-pgsql-fkeys
		#:create-indexes-in-kernel
                #:format-vector-row
                #:reset-sequences)
  (:export #:mssql-connection
           #:copy-mssql
           #:*mssql-default-cast-rules*
	   #:map-rows
	   #:copy-to
	   #:copy-from
	   #:copy-database))

(defpackage #:pgloader.mssql.index-filter
  (:use #:cl #:esrap #:pgloader.utils #:pgloader.mssql)
  (:import-from #:pgloader.pgsql
                #:translate-index-filter))

(defpackage #:pgloader.syslog
  (:use #:cl #:pgloader.params #:pgloader.utils)
  (:import-from #:pgloader.pgsql
		#:with-pgsql-transaction
		#:pgsql-execute)
  (:export #:stream-messages
	   #:start-syslog-server
	   #:send-message))


;;;
;;; the command parser
;;;
(defpackage #:pgloader.parser
  (:use #:cl #:esrap #:metabang.bind
        #:pgloader.params #:pgloader.utils #:pgloader.sql #:pgloader.connection)
  (:shadow #:namestring #:number #:inline)
  (:import-from #:alexandria #:read-file-into-string)
  (:import-from #:pgloader.sources
                #:md-connection
                #:md-spec)
  (:import-from #:pgloader.pgsql
                #:pgsql-connection
		#:with-pgsql-transaction
		#:pgsql-execute
                #:pgconn-use-ssl
                #:pgconn-table-name
                #:make-table)
  (:import-from #:pgloader.csv
                #:csv-connection
                #:specs
                #:csv-specs)
  (:import-from #:pgloader.fixed
                #:fixed-connection)
  (:import-from #:pgloader.copy
                #:copy-connection)
  (:import-from #:pgloader.sources
                #:*default-cast-rules*
                #:*cast-rules*)
  (:import-from #:pgloader.mysql
                #:mysql-connection
                #:*decoding-as*
                #:*mysql-default-cast-rules*)
  (:import-from #:pgloader.mssql
                #:mssql-connection
                #:*mssql-default-cast-rules*)
  (:import-from #:pgloader.sqlite
                #:sqlite-connection
                #:*sqlite-default-cast-rules*)
  (:import-from #:pgloader.db3 #:dbf-connection)
  (:import-from #:pgloader.ixf #:ixf-connection)
  (:export #:parse-commands
           #:parse-commands-from-file

           ;; tools to enable complete cli parsing in main.lisp
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
           #:parse-target-pg-db-uri

           ;; connection types / classes symbols for use in main
           #:connection
           #:conn-type
           #:csv-connection
           #:fixed-connection
           #:copy-connection
           #:dbf-connection
           #:ixf-connection
           #:sqlite-connection
           #:mysql-connection
           #:mssql-connection

           ;; functions to generate lisp code from parameters
           #:lisp-code-for-loading-from-mysql
           #:lisp-code-for-loading-from-csv
           #:lisp-code-for-loading-from-fixed
           #:lisp-code-for-loading-from-copy
           #:lisp-code-for-loading-from-dbf
           #:lisp-code-for-loading-from-ixf
           #:lisp-code-for-loading-from-sqlite
           #:lisp-code-for-loading-from-mssql))


;;
;; main package
;;
(defpackage #:pgloader
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.parser
        #:pgloader.connection #:metabang.bind)
  (:import-from #:pgloader.pgsql
                #:pgconn-table-name
                #:pgsql-connection
		#:copy-from-file)
  (:import-from #:pgloader.pgsql
                #:with-pgsql-connection
                #:with-schema
                #:list-reserved-keywords)
  (:export #:*version-string*
	   #:*state*
	   #:*fd-path-root*
	   #:*root-dir*
	   #:*pg-settings*
           #:*dry-run*
           #:*on-error-stop*

           #:load-data
           #:parse-source-string
           #:parse-source-string-for-type
           #:parse-target-string

	   #:run-commands
	   #:parse-commands
	   #:with-database-uri
	   #:slurp-file-into-string
	   #:copy-from-file))

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

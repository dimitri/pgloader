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
           #:parse-column-typemod
           #:typemod-expr-matches-p
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
  (:export #:apply-identifier-case
           #:build-identifier
           #:quoted-p
           #:ensure-quoted
           #:ensure-unquoted
           #:camelCase-to-colname))

(defpackage #:pgloader.catalog
  (:use #:cl #:pgloader.params #:pgloader.quoting)
  (:export #:format-create-sql
           #:format-drop-sql
           #:format-default-value

           #:catalog
           #:schema
           #:extension
           #:sqltype
           #:table
           #:matview
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
           #:make-matview
           #:make-sqltype
           #:make-column
           #:make-index
           #:make-fkey
           #:make-trigger
           #:make-procedure

           #:catalog-name
           #:catalog-schema-list
           #:catalog-types-without-btree
           #:catalog-distribution-rules

           #:schema-name
           #:schema-catalog
           #:schema-source-name
           #:schema-table-list
           #:schema-view-list
           #:schema-extension-list
           #:schema-sqltype-list
           #:schema-in-search-path

           #:table-name
           #:table-source-name
           #:table-schema
           #:table-oid
           #:table-comment
           #:table-storage-parameter-list
           #:table-tablespace
           #:table-row-count-estimate
           #:table-field-list
           #:table-column-list
           #:table-index-list
           #:table-fkey-list
           #:table-trigger-list
           #:table-citus-rule

           #:matview-name
           #:matview-source-name
           #:matview-schema
           #:matview-definition

           #:extension-name
           #:extension-schema

           #:sqltype-name
           #:sqltype-schema
           #:sqltype-type
           #:sqltype-source-def
           #:sqltype-extra
           #:sqltype-extension

           #:column-name
           #:column-type-name
           #:column-type-mod
           #:column-nullable
           #:column-default
           #:column-comment
           #:column-transform
           #:column-extra
           #:column-transform-default

           #:index-name
           #:index-type
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

           #:trigger-p
           #:trigger-name
           #:trigger-table
           #:trigger-action
           #:trigger-procedure

           #:procedure-schema
           #:procedure-name
           #:procedure-returns
           #:procedure-language
           #:procedure-body

           #:table-list
           #:view-list
           #:extension-list
           #:sqltype-list
           #:add-schema
           #:find-schema
           #:maybe-add-schema
           #:add-extension
           #:find-extension
           #:maybe-add-extension
           #:add-sqltype
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
           #:field-name

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

           #:citus-reference-rule
           #:citus-distributed-rule
           #:make-citus-reference-rule
           #:make-citus-distributed-rule
           #:citus-reference-rule-rule
           #:citus-distributed-rule-table
           #:citus-distributed-rule-using
           #:citus-distributed-rule-from
           #:citus-format-sql-select
           #:citus-backfill-table-p

           #:format-table-name))

(defpackage #:pgloader.state
  (:use #:cl #:pgloader.params #:pgloader.catalog)
  (:export #:create-state
           #:make-state
           #:state-preload
           #:state-data
           #:state-postload
           #:state-secs
           #:get-state-section

           #:make-pgstate
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
           #:pgtable-secs
           #:pgtable-rows
           #:pgtable-bytes
           #:pgtable-start
           #:pgtable-stop
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
           #:*monitoring-kernel*
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
           #:monitor-error
           #:elapsed-time-since
           #:timing))

(defpackage #:pgloader.queries
  (:use #:cl #:pgloader.params)
  (:export #:*queries*
           #:sql
           #:sql-url-for-variant))

(defpackage #:pgloader.citus
  (:use #:cl
        #:pgloader.params
        #:pgloader.catalog
        #:pgloader.quoting
        #:pgloader.monitor)
  (:export #:citus-distribute-schema
           #:citus-format-sql-select
           #:citus-backfill-table-p
           #:citus-rule-table-not-found
           #:citus-rule-is-missing-from-list

           #:citus-reference-rule
           #:citus-reference-rule-p
           #:citus-reference-rule-table

           #:citus-distributed-rule
           #:citus-distributed-rule-p
           #:citus-distributed-rule-table
           #:citus-distributed-rule-using
           #:citus-distributed-rule-from))

(defpackage #:pgloader.utils
  (:use #:cl
        #:pgloader.params
        #:pgloader.queries
        #:pgloader.quoting
        #:pgloader.catalog
        #:pgloader.monitor
        #:pgloader.state
        #:pgloader.citus)
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
           #:pretty-print-bytes
           #:split-range
           #:distribute

           ;; threads
           #:make-kernel

           ;; charsets
           #:list-encodings-and-aliases
           #:show-encodings
           #:make-external-format))

(eval-when (:compile-toplevel :load-toplevel :execute)
  (cl-user::export-inherited-symbols "pgloader.queries" "pgloader.utils")
  (cl-user::export-inherited-symbols "pgloader.quoting" "pgloader.utils")
  (cl-user::export-inherited-symbols "pgloader.catalog" "pgloader.utils")
  (cl-user::export-inherited-symbols "pgloader.monitor" "pgloader.utils")
  (cl-user::export-inherited-symbols "pgloader.state"   "pgloader.utils")
  (cl-user::export-inherited-symbols "pgloader.citus"   "pgloader.utils"))


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
        #:pgloader.params #:pgloader.utils #:pgloader.transforms
        #:pgloader.connection #:pgloader.catalog)
  (:import-from #:cl-postgres
                #:database-error-context)
  (:export #:pgsql-connection
           #:pgconn-use-ssl
           #:pgconn-table-name
           #:pgconn-version-string
           #:pgconn-major-version
           #:pgconn-variant
           #:with-pgsql-transaction
	   #:with-pgsql-connection
	   #:pgsql-execute
	   #:pgsql-execute-with-timing
	   #:pgsql-connect-and-execute-with-timing
           #:postgresql-unavailable
           #:postgresql-retryable
           #:with-disabled-triggers

           ;; postgresql schema facilities
	   #:truncate-tables
           #:set-table-oids

           #:create-extensions
           #:create-sqltypes
	   #:create-schemas
           #:add-to-search-path
	   #:create-tables
	   #:create-views
           #:drop-pgsql-fkeys
           #:create-pgsql-fkeys
           #:create-triggers

           #:fetch-pgsql-catalog
           #:merge-catalogs

	   #:create-indexes-in-kernel
           #:drop-indexes
           #:maybe-drop-indexes
           #:create-indexes-again
           #:reset-sequences
           #:comment-on-tables-and-columns

           #:create-distributed-table

           #:make-including-expr-from-catalog
           #:make-including-expr-from-view-names

           ;; finalizing catalogs support (redshift and other variants)
           #:finalize-catalogs
           #:adjust-data-types

           ;; index filter rewriting support
           #:translate-index-filter
           #:process-index-definitions

           ;; postgresql introspection queries
           #:list-all-sqltypes
	   #:list-all-columns
	   #:list-all-indexes
	   #:list-all-fkeys
	   #:list-missing-fk-deps
	   #:list-schemas
	   #:list-table-oids

           ;; postgresql identifiers
	   #:list-reserved-keywords
           #:list-typenames-without-btree-support

           ;; postgresql user provided gucs
           #:sanitize-user-gucs

           ;; postgresql data format
	   #:get-date-columns
           #:format-vector-row))


;;;
;;; pgloader Sources API and common helpers
;;;
(defpackage #:pgloader.sources
  (:use #:cl #:pgloader.params #:pgloader.utils #:pgloader.connection)
  (:import-from #:pgloader.transforms
                #:precision
                #:scale
                #:intern-symbol
                #:parse-column-typemod
                #:typemod-expr-matches-p
                #:typemod-expr-to-function)
  (:import-from #:pgloader.parse-date
                #:parse-date-string
                #:parse-date-format)
  (:export #:copy
           #:md-copy
           #:db-copy

           ;; main data access api
	   #:map-rows
           #:copy-column-list
           #:data-is-preformatted-p
           #:preprocess-row

           ;; accessors
	   #:source-db
	   #:target-db
	   #:source
	   #:target
	   #:fields
	   #:columns
	   #:transforms
           #:preprocessor
           #:copy-format
           #:columns-escape-mode
           #:encoding
           #:skip-lines
           #:header

           ;; md-copy protocol/api
           #:parse-header
           #:process-rows

           ;; the md-connection facilities
           #:md-connection
           #:md-spec
           #:md-strm
           #:expand-spec
           #:clone-copy-for

           ;; file based utils for csv, fixed etc
           #:with-open-file-or-stream
	   #:get-pathname
	   #:project-fields
	   #:reformat-then-process

           ;; the db-methods
           #:fetch-metadata
           #:cleanup
           #:instanciate-table-copy-object
           #:concurrency-support

           #:filter-list-to-where-clause
           #:fetch-columns
           #:fetch-indexes
           #:fetch-foreign-keys
           #:fetch-comments
           #:get-column-sql-expression
           #:get-column-list
           #:format-matview-name
           #:create-matviews
           #:drop-matviews

           ;; database cast machinery
           #:*default-cast-rules*
           #:*cast-rules*
           #:apply-casting-rules
           #:format-pgsql-type))


;;;
;;; COPY protocol related facilities
;;;
(defpackage #:pgloader.pgcopy
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.pgsql #:pgloader.sources)
  (:import-from #:cl-postgres
                #:database-error-context)
  (:import-from #:cl-postgres-trivial-utf-8
                #:utf-8-byte-length
                #:as-utf-8-bytes
                #:string-to-utf-8-bytes)
  (:export #:copy-rows-from-queue
           #:format-vector-row
           #:copy-init-error))



;;;
;;; The pgloader.load package implements data transfert from a pgloader
;;; source to a PostgreSQL database, using the pgloader.pgcopy COPY
;;; implementation.
;;;
(defpackage #:pgloader.load
  (:use #:cl #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.pgsql #:pgloader.pgcopy #:pgloader.sources)
  (:export
           ;; main protocol/api
           #:concurrency-support
           #:queue-raw-data
	   #:copy-from
	   #:copy-to
	   #:copy-database

           ;; the db-methods
           #:prepare-pgsql-database
           #:instanciate-table-copy-object
           #:complete-pgsql-database))


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
(defpackage #:pgloader.source.csv
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

(defpackage #:pgloader.source.fixed
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.sources)
  (:import-from #:pgloader.source.csv
                #:csv-connection
                #:specs
                #:csv-specs)
  (:import-from #:pgloader.pgsql
                #:maybe-drop-indexes
                #:create-indexes-again)
  (:export #:fixed-connection
           #:copy-fixed
	   #:copy-from))

(defpackage #:pgloader.source.copy
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.sources)
  (:import-from #:pgloader.source.csv
                #:csv-connection
                #:specs
                #:csv-specs)
  (:import-from #:pgloader.pgsql
                #:maybe-drop-indexes
                #:create-indexes-again)
  (:export #:copy-connection
           #:copy-copy
	   #:copy-from))

(defpackage #:pgloader.source.ixf
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

(defpackage #:pgloader.source.db3
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
           #:*db3-default-cast-rules*
           #:copy-db3
	   #:map-rows
	   #:copy-to
	   #:copy-from))

(defpackage #:pgloader.source.mysql
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
           #:with-mysql-connection))

(defpackage #:pgloader.source.pgsql
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.connection
        #:pgloader.sources #:pgloader.pgsql #:pgloader.catalog)
  (:import-from #:pgloader.transforms #:precision #:scale)
  (:export #:copy-pgsql
           #:*pgsql-default-cast-rules*))

(defpackage #:pgloader.source.sqlite
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
           #:*sqlite-default-cast-rules*))

(defpackage #:pgloader.source.mssql
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
           #:*mssql-default-cast-rules*))

(defpackage #:pgloader.source.mssql.index-filter
  (:use #:cl #:esrap #:pgloader.utils #:pgloader.source.mssql)
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
  (:import-from #:pgloader.load
                #:copy-database)
  (:import-from #:pgloader.sources
                #:md-connection
                #:md-spec
                #:*default-cast-rules*
                #:*cast-rules*)
  (:import-from #:pgloader.pgsql
                #:pgsql-connection
		#:with-pgsql-transaction
		#:pgsql-execute
                #:pgconn-use-ssl
                #:pgconn-table-name
                #:make-table)
  (:import-from #:pgloader.source.csv
                #:copy-csv
                #:csv-connection
                #:specs
                #:csv-specs)
  (:import-from #:pgloader.source.fixed
                #:copy-fixed
                #:fixed-connection)
  (:import-from #:pgloader.source.copy
                #:copy-copy
                #:copy-connection)
  (:import-from #:pgloader.source.pgsql
                #:copy-pgsql
                #:*pgsql-default-cast-rules*)
  (:import-from #:pgloader.source.mysql
                #:copy-mysql
                #:mysql-connection
                #:*decoding-as*
                #:*mysql-default-cast-rules*)
  (:import-from #:pgloader.source.mssql
                #:copy-mssql
                #:mssql-connection
                #:*mssql-default-cast-rules*)
  (:import-from #:pgloader.source.sqlite
                #:copy-sqlite
                #:sqlite-connection
                #:*sqlite-default-cast-rules*)
  (:import-from #:pgloader.source.db3
                #:copy-db3
                #:dbf-connection
                #:*db3-default-cast-rules*)
  (:import-from #:pgloader.source.ixf
                #:copy-ixf
                #:ixf-connection)
  (:export #:parse-commands
           #:parse-commands-from-file
           #:initialize-context
           #:execute-sql-code-block

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
           #:parse-sqlite-type-name

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
           #:lisp-code-for-loading-from-mssql
           #:lisp-code-for-loading-from-pgsql))


;;
;; main package
;;
(defpackage #:pgloader
  (:use #:cl
        #:pgloader.params #:pgloader.utils #:pgloader.parser
        #:pgloader.connection #:pgloader.pgcopy #:metabang.bind)
  (:import-from #:pgloader.pgsql
                #:pgconn-table-name
                #:pgsql-connection)
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

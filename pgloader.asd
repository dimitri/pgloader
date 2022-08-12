;;;; pgloader.asd

(asdf:defsystem #:pgloader
  :serial t
  :description "Load data into PostgreSQL"
  :author "Dimitri Fontaine <dim@tapoueh.org>"
  :license "The PostgreSQL Licence"
  :depends-on (#:uiop			; host system integration
               #:cl-log                 ; logging
               #:postmodern		; PostgreSQL protocol implementation
               #:cl-postgres		; low level bits for COPY streaming
               #:simple-date		; FIXME: recheck dependency
               #:qmynd                  ; MySQL protocol implemenation
               #:split-sequence         ; some parsing is made easy
               #:cl-csv                 ; full CSV reader
               #:cl-fad                 ; file and directories
               #:lparallel		; threads, workers, queues
               #:esrap                  ; parser generator
               #:alexandria		; utils
               #:drakma                 ; http client, download archives
               #:flexi-streams          ; streams
               #:usocket		; UDP / syslog
               #:local-time		; UDP date parsing
               #:command-line-arguments ; for the main function
               #:db3			; DBF version 3 file reader
               #:ixf			; IBM IXF file format reader
               #:py-configparser	; Read old-style INI config files
               #:sqlite                 ; Query a SQLite file
               #:cl-base64              ; Decode base64 data
               #:trivial-backtrace  	; For --debug cli usage
               #:cl-markdown            ; To produce the website
               #:metabang-bind          ; the bind macro
               #:mssql                  ; M$ SQL connectivity
               #:uuid               ; Transforming MS SQL unique identifiers
               #:quri               ; decode URI parameters
               #:cl-ppcre           ; Perl Compatible Regular Expressions
               #:cl-mustache        ; Logic-less templates
               #:yason              ; JSON routines
               #:closer-mop         ; introspection
               #:zs3                ; integration with AWS S3 for Redshift
               )
  :components
  ((:module "src"
            :components
            ((:file "params")
             (:file "package" :depends-on ("params"))

             (:module "monkey"
                      :components
                      ((:file "bind")
                       (:file "mssql")))

             (:module "utils"
                      :depends-on ("package" "params")
                      :components
                      ((:file "charsets")
                       (:file "logs")
                       (:file "utils")
                       (:file "state")

                       ;; user defined transforms package and pgloader
                       ;; provided ones
                       (:file "transforms")

                       ;; PostgreSQL related utils
                       (:file "read-sql-files")
                       (:file "queries")
                       (:file "quoting"     :depends-on ("utils"))
                       (:file "catalog"     :depends-on ("quoting"))
                       (:file "alter-table" :depends-on ("catalog"))
                       (:file "citus"       :depends-on ("catalog"))

                       ;; State, monitoring, reporting
                       (:file "reject"  :depends-on ("state"))
                       (:file "pretty-print-state" :depends-on ("state"))
                       (:file "report"  :depends-on ("state"
                                                     "pretty-print-state"
                                                     "utils"
                                                     "catalog"))
                       (:file "monitor" :depends-on ("logs"
                                                     "state"
                                                     "reject"
                                                     "report"))
                       (:file "threads" :depends-on ("monitor"))
                       (:file "archive" :depends-on ("monitor"))

                       ;; generic connection api
                       (:file "connection" :depends-on ("monitor"
                                                        "archive"))))

             ;; package pgloader.pgsql
             (:module pgsql
                      :depends-on ("package" "params" "utils")
                      :serial t
                      :components
                      ((:file "connection")
                       (:file "pgsql-ddl")
                       (:file "pgsql-ddl-citus")
                       (:file "pgsql-schema")
                       (:file "merge-catalogs" :depends-on ("pgsql-schema"))
                       (:file "pgsql-trigger")
                       (:file "pgsql-index-filter")
                       (:file "pgsql-finalize-catalogs")
                       (:file "pgsql-create-schema"
                              :depends-on ("pgsql-trigger"))))

             ;; Source format specific implementations
             (:module sources
                      :depends-on ("monkey" ; mssql driver patches
                                   "params"
                                   "package"
                                   "pgsql"
                                   "utils")
                      :components
                      ((:module "common"
                                :serial t
                                :components
                                ((:file "api")
                                 (:file "methods")
                                 (:file "md-methods")
                                 (:file "matviews")
                                 (:file "casting-rules")
                                 (:file "files-and-pathnames")
                                 (:file "project-fields")))

                       (:module "csv"
                                :depends-on ("common")
                                :components
                                ((:file "csv-guess")
                                 ;; (:file "csv-database")
                                 (:file "csv")))

                       (:module "fixed"
                                :depends-on ("common")
                                :serial t
                                :components
                                ((:file "fixed-guess")
                                 (:file "fixed")))

                       (:file "copy"
                              :depends-on ("common" "csv"))

                       (:module "db3"
                                :serial t
                                :depends-on ("common" "csv")
                                :components
                                ((:file "db3-cast-rules")
                                 (:file "db3-connection")
                                 (:file "db3-schema")
                                 (:file "db3")))

                       (:module "ixf"
                                :serial t
                                :depends-on ("common")
                                :components
                                ((:file "ixf-cast-rules")
                                 (:file "ixf-connection")
                                 (:file "ixf-schema")
                                 (:file "ixf" :depends-on ("ixf-schema"))))

                                        ;(:file "syslog") ; experimental...

                       (:module "sqlite"
                                :serial t
                                :depends-on ("common")
                                :components
                                ((:file "sqlite-cast-rules")
                                 (:file "sqlite-connection")
                                 (:file "sqlite-schema")
                                 (:file "sqlite")))

                       (:module "mssql"
                                :serial t
                                :depends-on ("common")
                                :components
                                ((:file "mssql-cast-rules")
                                 (:file "mssql-connection")
                                 (:file "mssql-schema")
                                 (:file "mssql")
                                 (:file "mssql-index-filters")))

                       (:module "mysql"
                                :serial t
                                :depends-on ("common")
                                :components
                                ((:file "mysql-cast-rules")
                                 (:file "mysql-connection")
                                 (:file "mysql-schema")
                                 (:file "mysql")))

                       (:module "pgsql"
                                :serial t
                                :depends-on ("common")
                                :components ((:file "pgsql-cast-rules")
                                             (:file "pgsql")))))

             ;; package pgloader.copy
             (:module "pg-copy"
                      :depends-on ("params"
                                   "package"
                                   "utils"
                                   "pgsql"
                                   "sources")
                      :serial t
                      :components
                      ((:file "copy-batch")
                       (:file "copy-format")
                       (:file "copy-db-write")
                       (:file "copy-rows-in-stream")
                       (:file "copy-rows-in-batch")
                       (:file "copy-rows-in-batch-through-s3")
                       (:file "copy-retry-batch")
                       (:file "copy-from-queue")))

             (:module "load"
                      :depends-on ("params"
                                   "package"
                                   "utils"
                                   "pgsql"
                                   "sources")
                      :serial t
                      :components
                      ((:file "api")
                       (:file "copy-data")
                       (:file "load-file")
                       (:file "migrate-database")))

             (:module "parsers"
                      :depends-on ("params"
                                   "package"
                                   "utils"
                                   "pgsql"
                                   "sources"
                                   "monkey")
                      :serial t
                      :components
                      ((:file "parse-ini")
                       (:file "template")
                       (:file "command-utils")
                       (:file "command-keywords")
                       (:file "command-regexp")
                       (:file "parse-pgpass")
                       (:file "command-db-uri")
                       (:file "command-source")
                       (:file "command-options")
                       (:file "command-sql-block")
                       (:file "command-sexp")
                       (:file "command-csv")
                       (:file "command-ixf")
                       (:file "command-fixed")
                       (:file "command-copy")
                       (:file "command-dbf")
                       (:file "command-cast-rules")
                       (:file "command-materialize-views")
                       (:file "command-alter-table")
                       (:file "command-distribute")
                       (:file "command-mysql")
                       (:file "command-including-like")
                       (:file "command-mssql")
                       (:file "command-sqlite")
                       (:file "command-pgsql")
                       (:file "command-archive")
                       (:file "command-parser")
                       (:file "parse-sqlite-type-name")
                       (:file "date-format")))

             ;; the main entry file, used when building a stand-alone
             ;; executable image
             (:file "api"  :depends-on ("params"
                                        "package"
                                        "utils"
                                        "parsers"
                                        "sources"))

             (:module "regress"
                      :depends-on ("params" "package" "utils" "pgsql" "api")
                      :components ((:file "regress")))


             (:file "main" :depends-on ("params"
                                        "package"
                                        "utils"
                                        "parsers"
                                        "sources"
                                        "api"
                                        "regress"))))))


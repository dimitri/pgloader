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
               #:abnf			; ABNF parser generator (for syslog)
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
                       (:file "batch")
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
                      ((:file "copy-format")
                       (:file "connection")
                       (:file "pgsql-ddl")
                       (:file "pgsql-schema")
                       (:file "merge-catalogs" :depends-on ("pgsql-schema"))
                       (:file "pgsql-trigger")
                       (:file "pgsql-index-filter")
                       (:file "pgsql-create-schema" :depends-on ("pgsql-trigger"))
                       (:file "retry-batch")
                       (:file "copy-from-queue"
                              :depends-on ("copy-format"
                                           "connection"
                                           "retry-batch"
                                           "pgsql-create-schema"
                                           "pgsql-schema"))))

             ;; Source format specific implementations
             (:module sources
                      :depends-on ("monkey" ; mssql driver patches
                                   "params"
                                   "package"
                                   "pgsql"
                                   "utils")
                      :components
                      ((:module "common"
                                :components
                                ((:file "api")
                                 (:file "methods"    :depends-on ("api"))
                                 (:file "md-methods" :depends-on ("api"))
                                 (:file "db-methods" :depends-on ("api"))
                                 (:file "casting-rules")
                                 (:file "files-and-pathnames")
                                 (:file "project-fields")))

                       (:module "csv"
                                :depends-on ("common")
                                :components
                                ((:file "csv-guess")
                                 ;; (:file "csv-database")
                                 (:file "csv")))

                       (:file "fixed"
                              :depends-on ("common" "csv"))

                       (:file "copy"
                              :depends-on ("common" "csv"))

                       (:module "db3"
                                :depends-on ("common" "csv")
                                :components
                                ((:file "db3-schema")
                                 (:file "db3" :depends-on ("db3-schema"))))

                       (:module "ixf"
                                :depends-on ("common")
                                :components
                                ((:file "ixf-schema")
                                 (:file "ixf" :depends-on ("ixf-schema"))))

                                        ;(:file "syslog") ; experimental...

                       (:module "sqlite"
                                :depends-on ("common")
                                :components
                                ((:file "sqlite-cast-rules")
                                 (:file "sqlite-schema"
                                        :depends-on ("sqlite-cast-rules"))
                                 (:file "sqlite"
                                        :depends-on ("sqlite-cast-rules"
                                                     "sqlite-schema"))))

                       (:module "mssql"
                                :depends-on ("common")
                                :components
                                ((:file "mssql-cast-rules")
                                 (:file "mssql-schema"
                                        :depends-on ("mssql-cast-rules"))
                                 (:file "mssql"
                                        :depends-on ("mssql-cast-rules"
                                                     "mssql-schema"))
                                 (:file "mssql-index-filters"
                                        :depends-on ("mssql"))))

                       (:module "mysql"
                                :depends-on ("common")
                                :components
                                ((:file "mysql-cast-rules")
                                 (:file "mysql-connection")
                                 (:file "mysql-schema"
                                        :depends-on ("mysql-connection"
                                                     "mysql-cast-rules"))
                                 ;; (:file "mysql-csv"
                                 ;;        :depends-on ("mysql-schema"))
                                 (:file "mysql"
                                        :depends-on ("mysql-cast-rules"
                                                     "mysql-schema"))))))

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
                       (:file "command-mysql")
                       (:file "command-including-like")
                       (:file "command-mssql")
                       (:file "command-sqlite")
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


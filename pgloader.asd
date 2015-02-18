;;;; pgloader.asd

(asdf:defsystem #:pgloader
    :serial t
    :description "Load data into PostgreSQL"
    :author "Dimitri Fontaine <dimitri@2ndQuadrant.fr>"
    :license "The PostgreSQL Licence"
    :depends-on (#:uiop			; host system integration
		 #:cl-log		; logging
		 #:postmodern		; PostgreSQL protocol implementation
		 #:cl-postgres		; low level bits for COPY streaming
		 #:simple-date		; FIXME: recheck dependency
		 #:qmynd		; MySQL protocol implemenation
		 #:split-sequence	; some parsing is made easy
		 #:cl-csv		; full CSV reader
		 #:cl-fad		; file and directories
		 #:lparallel		; threads, workers, queues
		 #:esrap		; parser generator
		 #:alexandria		; utils
		 #:drakma		; http client, download archives
		 #:flexi-streams	; streams
		 #:usocket		; UDP / syslog
		 #:local-time		; UDP date parsing
		 #:command-line-arguments ; for the main function
		 #:abnf			; ABNF parser generator (for syslog)
		 #:db3			; DBF version 3 file reader
		 #:ixf			; IBM IXF file format reader
		 #:py-configparser	; Read old-style INI config files
		 #:sqlite		; Query a SQLite file
                 #:cl-base64            ; Decode base64 data
		 #:trivial-backtrace  	; For --debug cli usage
                 #:cl-markdown          ; To produce the website
                 #:metabang-bind        ; the bind macro
                 #:mssql                ; M$ SQL connectivity
                 #:uuid             ; Transforming MS SQL unique identifiers
		 )
    :components
    ((:module "src"
	      :components
	      ((:file "params")
	       (:file "package" :depends-on ("params"))
               (:file "queue"   :depends-on ("params" "package"))

               (:module "monkey"
                        :components
                        ((:file "bind")
                         (:file "mssql")))

               (:module "utils"
                        :depends-on ("package" "params")
                        :components
                        ((:file "charsets")
                         (:file "threads")
                         (:file "logs")
                         (:file "monitor" :depends-on ("logs"))
                         (:file "state")
                         (:file "report"  :depends-on ("state"))
                         (:file "utils"   :depends-on ("charsets" "monitor"))
                         (:file "archive" :depends-on ("logs"))

                         ;; those are one-package-per-file
                         (:file "transforms")
                         (:file "read-sql-files")))

               ;; generic connection api
               (:file "connection" :depends-on ("utils"))

	       ;; package pgloader.pgsql
	       (:module pgsql
			:depends-on ("package" "params" "utils" "connection")
			:components
			((:file "copy-format")
			 (:file "queries")
			 (:file "schema")
			 (:file "pgsql"
				:depends-on ("copy-format"
                                             "queries"
                                             "schema"))))

               (:module "parsers"
                        :depends-on ("params" "package" "utils"
                                              "pgsql" "monkey" "connection")
                        :serial t
                        :components
                        ((:file "parse-ini")
                         (:file "command-utils")
                         (:file "command-keywords")
                         (:file "command-regexp")
                         (:file "command-db-uri")
                         (:file "command-source")
                         (:file "command-options")
                         (:file "command-sql-block")
                         (:file "command-csv")
                         (:file "command-ixf")
                         (:file "command-fixed")
                         (:file "command-copy")
                         (:file "command-dbf")
                         (:file "command-cast-rules")
                         (:file "command-mysql")
                         (:file "command-mssql")
                         (:file "command-sqlite")
                         (:file "command-archive")
                         (:file "command-parser")
                         (:file "date-format")))

	       ;; Source format specific implementations
	       (:module sources
			:depends-on ("monkey"  ; mssql driver patches
                                     "params"
                                     "package"
                                     "connection"
                                     "pgsql"
                                     "utils"
                                     "parsers"
                                     "queue")
			:components
                        ((:module "common"
                                  :components
                                  ((:file "api")
                                   (:file "casting-rules")
                                   (:file "files-and-pathnames")
                                   (:file "project-fields")))

                         (:module "csv"
                                  :depends-on ("common")
                                  :components
                                  ((:file "csv-guess")
                                   (:file "csv-database")
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
                                                       "mssql-schema"))))

                         (:module "mysql"
                                  :depends-on ("common")
                                  :components
                                  ((:file "mysql-cast-rules")
                                   (:file "mysql-schema"
                                          :depends-on ("mysql-cast-rules"))
                                   (:file "mysql-csv"
                                          :depends-on ("mysql-schema"))
                                   (:file "mysql"
                                          :depends-on ("mysql-cast-rules"
                                                       "mysql-schema"))))))

	       ;; the main entry file, used when building a stand-alone
	       ;; executable image
	       (:file "main" :depends-on ("params"
                                          "package"
                                          "utils"
                                          "parsers"
                                          "sources"))))

     ;; to produce the website
     (:module "web"
              :components
              ((:module src
                        :components
                        ((:file "docs")))))))


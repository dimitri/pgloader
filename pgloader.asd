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
		 )
    :components
    ((:module "src"
	      :components
	      ((:file "params")
	       (:file "package" :depends-on ("params"))
               (:file "queue"   :depends-on ("params" "package"))

               (:module "monkey"
                        :components
                        ((:file "bind")))

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

	       ;; package pgloader.pgsql
	       (:module pgsql
			:depends-on ("package" "params" "utils")
			:components
			((:file "copy-format")
			 (:file "queries")
			 (:file "schema")
			 (:file "pgsql"
				:depends-on ("copy-format"
                                             "queries"
                                             "schema"))))

               (:module "parsers"
                        :depends-on ("params" "package" "utils" "pgsql" "monkey")
                        :components
                        ((:file "parse-ini")
                         (:file "parser")
                         (:file "date-format")))

               ;; generic API for Sources
               (:file "sources-api"
                      :pathname "sources"
                      :depends-on ("params" "package" "utils" "parsers"))

	       ;; Source format specific implementations
	       (:module sources
			:depends-on ("params"
                                     "package"
                                     "sources-api"
                                     "pgsql"
                                     "utils"
                                     "queue")
			:components
			((:file "csv")
			 (:file "fixed")
			 (:file "db3")
			 (:file "ixf")
			 (:file "sqlite")
			 (:file "syslog")

                         (:module "mysql-utils"
                                  :pathname "mysql"
                                  :components
                                  ((:file "mysql-cast-rules")
                                   (:file "mysql-schema")
                                   (:file "mysql-csv"
                                          :depends-on ("mysql-schema"))))

			 (:file "mysql" :depends-on ("mysql-utils"))))

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


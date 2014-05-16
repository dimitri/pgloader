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
		 #:zip			; support for zip archive files
		 #:flexi-streams	; streams
		 #:usocket		; UDP / syslog
		 #:local-time		; UDP date parsing
		 #:command-line-arguments ; for the main function
		 #:abnf			; ABNF parser generator (for syslog)
		 #:db3			; DBF version 3 file reader
		 #:py-configparser	; Read old-style INI config files
		 #:sqlite		; Query a SQLite file
                 #:cl-base64            ; Decode base64 data
		 #:trivial-backtrace  	; For --debug cli usage
                 #:cl-markdown          ; To produce the website
		 )
    :components
    ((:module "src"
	      :components
	      ((:file "params")

	       (:file "package" :depends-on ("params"))

	       (:file "logs"    :depends-on ("package" "params"))

	       (:file "monitor" :depends-on ("params"
                                             "package"
                                             "logs"))

	       (:file "charsets":depends-on ("package"))
	       (:file "utils"   :depends-on ("params"
                                             "package"
                                             "charsets"
                                             "monitor"))

	       ;; those are one-package-per-file
	       (:file "transforms")
	       (:file "queue"     :depends-on ("params" "package"))

	       (:file "parser"    :depends-on ("package"
                                               "params"
                                               "transforms"
                                               "utils"
                                               "monitor"
                                               "pgsql"))

	       (:file "parse-ini" :depends-on ("package"
                                               "params"
                                               "utils"))

	       (:file "archive"   :depends-on ("params"
                                               "package"
                                               "utils"
                                               "sources"
                                               "pgsql"))

	       ;; package pgloader.pgsql
	       (:module pgsql
			:depends-on ("package"
                                     "params"
                                     "queue"
                                     "utils"
                                     "logs"
                                     "monitor")
			:components
			((:file "copy-format")
			 (:file "queries")
			 (:file "schema")
			 (:file "pgsql"
				:depends-on ("copy-format"
                                             "queries"
                                             "schema"))))

	       ;; Source format specific implementations
	       (:module sources
			:depends-on ("params"
                                     "package"
                                     "pgsql"
                                     "utils"
                                     "logs"
                                     "monitor"
                                     "queue"
                                     "transforms")
			:components
			((:file "sources")
			 (:file "csv"     :depends-on ("sources"))
			 (:file "fixed"   :depends-on ("sources"))
			 (:file "db3"     :depends-on ("sources"))
			 (:file "sqlite"  :depends-on ("sources"))
			 (:file "syslog"  :depends-on ("sources"))
			 (:file "mysql-cast-rules")
			 (:file "mysql-schema")
			 (:file "mysql-csv" :depends-on ("mysql-schema"))
			 (:file "mysql" :depends-on ("mysql-cast-rules"
						     "mysql-schema"))))

	       ;; the main entry file, used when building a stand-alone
	       ;; executable image
	       (:file "main" :depends-on ("params"
                                          "package"
                                          "monitor"
                                          "utils"
                                          "parser"
                                          "sources"))))

     ;; to produce the website
     (:module "web"
              :components
              ((:module src
                        :components
                        ((:file "docs")))))))


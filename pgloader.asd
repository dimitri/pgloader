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
		 #:com.informatimago.clext ; portable character-sets listings
		 #:usocket		; UDP / syslog
		 #:local-time		; UDP date parsing
		 #:command-line-arguments ; for the main function
		 #:abnf			; ABNF parser generator (for syslog)
		 #:db3			; DBF version 3 file reader
		 #:py-configparser	; Read old-style INI config files
		 #:sqlite		; Query a SQLite file
		 #:trivial-backtrace  	; For --debug cli usage
                 #:cl-markdown          ; To produce the website
		 )
    :components
    ((:module "src"
	      :components
	      ((:file "params")
	       (:file "package" :depends-on ("params"))
	       (:file "logs"    :depends-on ("package"))
	       (:file "monitor" :depends-on ("package" "logs"))
	       (:file "utils"   :depends-on ("package"))

	       ;; those are one-package-per-file
	       (:file "transforms")
	       (:file "parser"    :depends-on ("package" "params" "transforms"))
	       (:file "parse-ini" :depends-on ("package" "params"))
	       (:file "queue"     :depends-on ("package"))
	       (:file "archive"   :depends-on ("sources" "pgsql"))

	       ;; package pgloader.pgsql
	       (:module pgsql
			:depends-on ("package" "params" "queue" "utils")
			:components
			((:file "copy-format")
			 (:file "queries")
			 (:file "schema")
			 (:file "pgsql"
				:depends-on ("copy-format" "queries" "schema"))))

	       ;; Source format specific implementations
	       (:module sources
			:depends-on ("package" "pgsql" "utils" "queue" "transforms")
			:components
			((:file "sources")
			 (:file "csv"     :depends-on ("sources"))
			 (:file "fixed"   :depends-on ("sources"))
			 (:file "db3"     :depends-on ("sources"))
			 (:file "sqlite"  :depends-on ("sources"))
			 (:file "syslog"  :depends-on ("sources"))
			 (:file "mysql-cast-rules")
			 (:file "mysql-schema")
			 (:file "mysql" :depends-on ("mysql-cast-rules"
						     "mysql-schema"))))

	       ;; the main entry file, used when building a stand-alone
	       ;; executable image
	       (:file "main" :depends-on ("package" "parser" "sources"))))

     ;; to produce the website
     (:module "web"
              :components
              ((:module src
                        :components
                        ((:file "docs")))))))


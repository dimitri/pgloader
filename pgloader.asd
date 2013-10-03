;;;; pgloader.asd

(asdf:defsystem #:pgloader
  :serial t
  :description "Load data into PostgreSQL"
  :author "Dimitri Fontaine <dimitri@2ndQuadrant.fr>"
  :license "The PostgreSQL Licence"
  :depends-on (#:uiop			; host system integration
	       #:cl-log			; logging
	       #:postmodern		; PostgreSQL protocol implementation
	       #:cl-postgres		; low level bits for COPY streaming
	       #:simple-date		; FIXME: recheck dependency
	       #:cl-mysql		; CFFI binding to libmysqlclient-dev
	       #:split-sequence		; some parsing is made easy
               #:cl-csv			; full CSV reader
	       #:cl-fad			; file and directories
               #:lparallel		; threads, workers, queues
	       #:esrap			; parser generator
	       #:alexandria		; utils
	       #:drakma			; http client, download archives
	       #:zip			; support for zip archive files
	       #:flexi-streams		; streams
	       #:com.informatimago.clext ; portable character-sets listings
	       #:usocket		; UDP / syslog
	       #:local-time		; UDP date parsing
	       #:command-line-arguments	; for the main function
	       #:abnf			; ABNF parser generator (for syslog)
	       #:db3			; DBF version 3 file reader
	       )
  :components ((:file "params")
	       (:file "package" :depends-on ("params"))
	       (:file "utils"   :depends-on ("package"))

	       ;; those are one-package-per-file
	       (:file "transforms")
	       (:file "parser"  :depends-on ("package" "params" "transforms"))
	       (:file "queue"   :depends-on ("package")) ; pgloader.queue

	       ;; package pgloader.pgsql
	       (:file "pgsql"   :depends-on ("package"
					     "queue"
					     "utils"
					     "transforms"))

	       ;; Source format specific implementations
	       (:file "csv"     :depends-on ("package" "pgsql"))
	       (:file "db3"     :depends-on ("package" "pgsql"))
	       (:file "archive" :depends-on ("package" "pgsql"))
	       (:file "syslog"  :depends-on ("package" "pgsql"))

	       ;; mysql.lisp depends on pgsql.lisp to be able to export data
	       ;; from MySQL in the PostgreSQL format.
	       ;;
	       ;; package pgloader.mysql
	       (:file "mysql-cast-rules" :depends-on ("package" "utils"))
	       (:file "mysql" :depends-on ("package"
					   "pgsql"
					   "queue"
					   "transforms"
					   "mysql-cast-rules"
					   "utils"))

	       ;; the main entry file, used when building a stand-alone
	       ;; executable image
	       (:file "main" :depends-on ("package"
					  "parser"
					  "csv"
					  "db3"
					  "archive"
					  "syslog"
					  "mysql"))))



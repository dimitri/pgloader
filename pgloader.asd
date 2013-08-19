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
               #:lparallel		; threads, workers, queues
	       #:esrap			; parser generator
	       #:alexandria		; utils
	       #:zip			; support for zip archive files
	       #:flexi-streams		; streams
	       #:command-line-arguments	; for the main function
	       #:uiop			; portability layer (quit, argv, etc)
	       )
  :components ((:file "params")
	       (:file "package" :depends-on ("params"))
	       (:file "utils"  :depends-on ("package"))

	       ;; those are one-package-per-file
	       (:file "parser" :depends-on ("package" "params"))
	       (:file "transforms")
	       (:file "queue" :depends-on ("package"))   ; pgloader.queue
	       (:file "csv"  :depends-on ("package"))    ; pgloader.csv
	       (:file "archive" :depends-on ("package")) ; pgloader.archive

	       ;; package pgloader.pgsql
	       (:file "pgsql" :depends-on ("package"
					   "queue"
					   "utils"
					   "transforms"))

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
					   "utils"))))



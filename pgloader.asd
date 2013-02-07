;;;; pgloader.asd

(asdf:defsystem #:pgloader
  :serial t
  :description "Load data into PostgreSQL"
  :author "Dimitri Fontaine <dimitri@2ndQuadrant.fr>"
  :license "The PostgreSQL Licence"
  :depends-on (#:postmodern
	       #:cl-postgres
	       #:simple-date
	       #:cl-mysql
	       #:split-sequence
               #:cl-csv
               #:lparallel)
  :components ((:file "params")
	       (:file "package" :depends-on ("params"))
	       (:file "utils"  :depends-on ("package"))
	       (:file "pgloader" :depends-on ("package" "utils"))

	       ;; those are one-package-per-file
	       (:file "queue" :depends-on ("package")) ; package pgloader.queue
	       (:file "csv"  :depends-on ("package"))  ; package pgloader.csv

	       ;; package pgloader.pgsql
	       (:file "pgsql" :depends-on ("package" "queue" "utils"))

	       ;; mysql.lisp depends on pgsql.lisp to be able to export data
	       ;; from MySQL in the PostgreSQL format.
	       ;;
	       ;; package pgloader.mysql
	       (:file "mysql" :depends-on ("package" "pgsql" "queue" "utils"))))



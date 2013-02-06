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
  :components ((:file "package")
	       (:file "utils"  :depends-on ("package"))
	       (:file "pgloader" :depends-on ("package"))

	       ;; those are one-package-per-file
	       (:file "queue")		             ; package pgloader.queue
	       (:file "csv")		             ; package pgloader.csv

	       ;; package pgloader.pgsql
	       (:file "pgsql" :depends-on ("queue" "utils"))

	       ;; mysql.lisp depends on pgsql.lisp to be able to export data
	       ;; from MySQL in the PostgreSQL format.
	       ;;
	       ;; package pgloader.mysql
	       (:file "mysql" :depends-on ("pgsql" "queue" "utils"))))



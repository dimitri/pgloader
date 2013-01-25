;;;; galaxya-loader.asd

(asdf:defsystem #:galaxya-loader
  :serial t
  :description "Load data from MySQL directly to PostgreSQL"
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
	       (:file "timing" :depends-on ("package"))
	       (:file "galaxya-export" :depends-on ("package"))
               (:file "galaxya-loader" :depends-on ("package"
						    "galaxya-export"))))


(defsystem #:pgloader-abcl
  :description "Load data into PostgreSQL"
  :author "Dimitri Fontaine <dim@tapoueh.org>, easye"
  :license "The PostgreSQL Licence")


(defsystem pgloader-abcl/jdbc/postgres
  :components ((:module jar :components
                        ((:mvn "org.postgresql/postgresql/42.2.16")))))


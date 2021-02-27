(asdf:defsystem #:pgloader-ci
  :serial t
  :description "CI Config for PostgreSQL"
  :author "Dimitri Fontaine <dim@tapoueh.org>"
  :license "The PostgreSQL Licence"
  :depends-on (#:40ants-ci)
  :components
  ((:module "src"
            :components
            ((:file "ci")))))


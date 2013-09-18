;;;; abnf.asd

(asdf:defsystem #:db3
  :serial t
  :description "DB3 file reader"
  :author "Xach"
  :license "WTFPL"
  :depends-on ()
  :components ((:file "package")
	       (:file "db3"  :depends-on ("package"))))



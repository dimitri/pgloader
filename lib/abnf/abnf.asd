;;;; abnf.asd

(asdf:defsystem #:abnf
  :serial t
  :description "ABNF Parser Generator, per RFC2234"
  :author "Dimitri Fontaine <dim@tapoueh.org>"
  :license "WTFPL"
  :depends-on (#:esrap			; parser generator
	       #:cl-ppcre		; regular expression
	       )
  :components ((:file "package")
	       (:file "abnf"  :depends-on ("package"))))



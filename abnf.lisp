;;;
;;; Parsing ABNF syntaxes so that we can offer users to edit them
;;;
;;; see http://tools.ietf.org/html/draft-ietf-syslog-protocol-15#page-10
;;; and http://tools.ietf.org/html/rfc2234
;;;

(defpackage #:pgloader.abnf
  (:use #:cl #:esrap))

(in-package :pgloader.abnf)

#|
      SYSLOG-MSG      = HEADER SP STRUCTURED-DATA [SP MSG]

      HEADER          = VERSION SP FACILITY SP SEVERITY SP
                        TRUNCATE SP TIMESTAMP SP HOSTNAME
                        SP APP-NAME SP PROCID SP MSGID
      VERSION         = NONZERO-DIGIT 0*2DIGIT
      FACILITY        = "0" / (NONZERO-DIGIT 0*9DIGIT)
                        ; range 0..2147483647
      SEVERITY        = "0" / "1" / "2" / "3" / "4" / "5" /
                        "6" / "7"
      TRUNCATE        = 2DIGIT
      HOSTNAME        = 1*255PRINTUSASCII

      APP-NAME        = 1*48PRINTUSASCII
      PROCID          = "-" / 1*128PRINTUSASCII
      MSGID           = "-" / 1*32PRINTUSASCII

      TIMESTAMP       = FULL-DATE "T" FULL-TIME
      FULL-DATE       = DATE-FULLYEAR "-" DATE-MONTH "-" DATE-MDAY
      DATE-FULLYEAR   = 4DIGIT
      DATE-MONTH      = 2DIGIT  ; 01-12
      DATE-MDAY       = 2DIGIT  ; 01-28, 01-29, 01-30, 01-31 based on
                                ; month/year
      FULL-TIME       = PARTIAL-TIME TIME-OFFSET
      PARTIAL-TIME    = TIME-HOUR ":" TIME-MINUTE ":" TIME-SECOND
                        [TIME-SECFRAC]
      TIME-HOUR       = 2DIGIT  ; 00-23
      TIME-MINUTE     = 2DIGIT  ; 00-59
      TIME-SECOND     = 2DIGIT  ; 00-58, 00-59, 00-60 based on leap
                                ; second rules
      TIME-SECFRAC    = "." 1*6DIGIT
      TIME-OFFSET     = "Z" / TIME-NUMOFFSET
      TIME-NUMOFFSET  = ("+" / "-") TIME-HOUR ":" TIME-MINUTE


      STRUCTURED-DATA = 1*SD-ELEMENT / "-"
      SD-ELEMENT      = "[" SD-ID *(SP SD-PARAM) "]"
      SD-PARAM        = PARAM-NAME "=" %d34 PARAM-VALUE %d34
      SD-ID           = SD-NAME
      PARAM-NAME      = SD-NAME
      PARAM-VALUE     = UTF-8-STRING ; characters '"', '\' and
                                     ; ']' MUST be escaped.
      SD-NAME         = 1*32PRINTUSASCII
                        ; except '=', SP, ']', %d34 (")

      MSG             = UTF-8-STRING
      UTF-8-STRING    = *OCTET ; Any VALID UTF-8 String
                        ; "shortest form" MUST be used

      OCTET           = %d00-255
      SP              = %d32
      PRINTUSASCII    = %d33-126
      NONZERO-DIGIT   = "1" / "2" / "3" / "4" / "5" /
                        "6" / "7" / "8" / "9"
      DIGIT           = "0" / NONZERO-DIGIT
|#

(defun rule-name-character-p (character)
  (or (alpha-char-p character)
      (char= character #\-)))

(defun vcharp (character)
  (<= #x21 (char-code character) #x7E))

(defrule vchar (+ (vcharp character))                   (:text t))
(defrule wsp (or #\Space #\Tab)                         (:constant :wsp))

(defrule comment (and ";" (* (or wsp vchar)) #\Newline) (:constant :comment))
(defrule c-nl (or comment #\Newline)                    (:constant :c-nl))
(defrule c-wsp (or (* wsp) c-nl)                        (:constant :c-wsp))

(defrule rule-name (+ (rule-name-character-p character))
  (:lambda (name)
    (intern (string-upcase (text name)) :pgloader.abnf)))

(defrule equal (and c-wsp #\= c-wsp)       (:constant :equal))
(defrule end-of-rule (and c-wsp #\Newline) (:constant :eor))

(defrule digit (digit-char-p character)
  (:lambda (digit)
    (parse-integer (text digit))))

(defrule digits (+ (digit-char-p character))
  (:lambda (digits)
    (parse-integer (text digits))))

(defun char-val-char-p (character)
  (let ((code (char-code character)))
    (or (<= #x20 code #x21)
	(<= #x23 code #x7E))))

(defrule char-val (and #\" (* (char-val-char-p character)) #\")
  (:lambda (char)
    (destructuring-bind (open val close) char
      (declare (ignore open close))
      (text val))))

(defrule dec-val (and "d" digits)
  (:lambda (dv)
    (destructuring-bind (d x) dv
      (declare (ignore d))
      x)))

(defun hexadecimal-char-p (character)
  (member character #. (quote (coerce "0123456789abcdef" 'list))))

(defrule hex-val (and "x" (+ (hexadecimal-char-p character)))
  (:lambda (hx)
    (parse-integer (text hx) :start 1 :radix 16)))

(defrule num-val (and "%" (or dec-val hex-val))
  (:lambda (nv)
    (destructuring-bind (percent val) nv
      (declare (ignore percent))
      val)))

(defrule element (or rule-name char-val num-val))

(defrule repeat-option (and "*" digit)
  (:lambda (rs)
    (destructuring-bind (star digit) rs
      (declare (ignore star))
      (cons 0 digit))))

(defrule repeat-var (and digits "*" digits)
  (:lambda (rv)
    (destructuring-bind (min star max) rv
      (declare (ignore star))
      (cons min max))))

(defrule repeat-specific digits
  (:lambda (digits)
    (cons digits digits)))

(defrule repeat (or repeat-option repeat-var repeat-specific))

(defrule repetition (and (? repeat) element)
  (:lambda (repetition)
    (destructuring-bind (repeat element) repetition
	(if repeat
	    (destructuring-bind (min . max) repeat
	      `(:non-greedy-repetition ,min ,max ,element))
	    ;; no repeat clause
	    element))))

(defrule concatenation-element (and c-wsp repetition)
  (:lambda (ce)
    (destructuring-bind (c-wsp rep) ce
      (declare (ignore c-wsp))
      rep)))

(defrule concatenation (and repetition (+ concatenation-element))
  (:lambda (concat)
    (destructuring-bind (rep1 rlist) concat
      `(:sequence ,@(list* rep1 rlist)))))

(defrule alternation-element (and c-wsp "/" c-wsp concatenation)
  (:lambda (ae)
    (destructuring-bind (ws1 sl ws2 concatenation) ae
      (declare (ignore ws1 sl ws2))
      concatenation)))

(defrule alternation (and concatenation (* alternation-element))
  (:lambda (alternation)
    (destructuring-bind (c1 clist) alternation
      (if clist
	  `(:alternation ,(list* c1 clist))
	  c1))))

(defrule group (and "(" c-wsp alternation c-wsp ")")
  (:lambda (group)
    (destructuring-bind (open ws1 a ws2 close) group
      (declare (ignore open close ws1 ws2))
      `(:group ,a))))

(defrule option (and "[" c-wsp alternation c-wsp "]")
  (:lambda (option)
    (destructuring-bind (open ws1 a ws2 close) option
      (declare (ignore open close ws1 ws2))
      `(:non-greedy-repetition 0 1 ,a))))

(defrule toplevel-element (or alternation group option))

(defrule elements (and (+ (and toplevel-element c-wsp)) end-of-rule)
  (:lambda (elist)
    (destructuring-bind (elements eor) elist
      (declare (ignore eor))
      `(:sequence ,@(concatenate 'list (mapcar #'car elements))))))

(defrule rule (and rule-name equal elements)
  (:lambda (rule)
    (destructuring-bind (rule-name eq definition) rule
      (declare (ignore eq))
      (cons rule-name definition))))

(defrule rule-list (+ rule))

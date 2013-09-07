;;;
;;; Parsing ABNF syntaxes so that we can offer users to edit them
;;;
;;; see http://tools.ietf.org/html/draft-ietf-syslog-protocol-15#page-10
;;; and http://tools.ietf.org/html/rfc2234
;;;

(defpackage #:pgloader.abnf
  (:use #:cl #:esrap))

(in-package :pgloader.abnf)

(defconstant +abnf-rfc-syslog-draft-15+
  "SYSLOG-MSG      = HEADER SP STRUCTURED-DATA [SP MSG]

   HEADER          = VERSION SP FACILITY SP SEVERITY SP
                     TRUNCATE SP TIMESTAMP SP HOSTNAME
                     SP APP-NAME SP PROCID SP MSGID
   VERSION         = NONZERO-DIGIT 0*2DIGIT
   FACILITY        = \"0\" / (NONZERO-DIGIT 0*9DIGIT)
                         ; range 0..2147483647 ;
   SEVERITY        = \"0\" / \"1\" / \"2\" / \"3\" / \"4\" / \"5\" /
   \"6\" / \"7\"
   TRUNCATE        = 2DIGIT
   HOSTNAME        = 1*255PRINTUSASCII

   APP-NAME        = 1*48PRINTUSASCII
   PROCID          = \"-\" / 1*128PRINTUSASCII
   MSGID           = \"-\" / 1*32PRINTUSASCII

   TIMESTAMP       = FULL-DATE \"T\" FULL-TIME
   FULL-DATE       = DATE-FULLYEAR \"-\" DATE-MONTH \"-\" DATE-MDAY
   DATE-FULLYEAR   = 4DIGIT
   DATE-MONTH      = 2DIGIT  ; 01-12
   DATE-MDAY       = 2DIGIT  ; 01-28, 01-29, 01-30, 01-31 based on
                                 ; month/year ;
   FULL-TIME       = PARTIAL-TIME TIME-OFFSET
   PARTIAL-TIME    = TIME-HOUR \":\" TIME-MINUTE \":\" TIME-SECOND
                     [TIME-SECFRAC]
   TIME-HOUR       = 2DIGIT  ; 00-23
   TIME-MINUTE     = 2DIGIT  ; 00-59
   TIME-SECOND     = 2DIGIT  ; 00-58, 00-59, 00-60 based on leap
                                 ; second rules ;
   TIME-SECFRAC    = \".\" 1*6DIGIT
   TIME-OFFSET     = \"Z\" / TIME-NUMOFFSET
   TIME-NUMOFFSET  = (\"+\" / \"-\") TIME-HOUR \":\" TIME-MINUTE


   STRUCTURED-DATA = 1*SD-ELEMENT / \"-\"
   SD-ELEMENT      = \"[\" SD-ID *(SP SD-PARAM) \"]\"
   SD-PARAM        = PARAM-NAME \"=\" %d34 PARAM-VALUE %d34
   SD-ID           = SD-NAME
   PARAM-NAME      = SD-NAME
   PARAM-VALUE     = UTF-8-STRING ; characters '\"', '\' and
                                      ; ']' MUST be escaped. ;
   SD-NAME         = 1*32PRINTUSASCII
                         ; except '=', SP, ']', %d34 (\") ;

   MSG             = UTF-8-STRING
   UTF-8-STRING    = *OCTET ; Any VALID UTF-8 String
                         ; \"shortest form\" MUST be used ;

   OCTET           = %d00-255
   SP              = %d32
   PRINTUSASCII    = %d33-126
   NONZERO-DIGIT   = \"1\" / \"2\" / \"3\" / \"4\" / \"5\" /
                     \"6\" / \"7\" / \"8\" / \"9\"
   DIGIT           = \"0\" / NONZERO-DIGIT"
  "See http://tools.ietf.org/html/draft-ietf-syslog-protocol-15#page-10")

#|

This table comes from http://tools.ietf.org/html/rfc2234#page-11 and 12.

        ALPHA          =  %x41-5A / %x61-7A   ; A-Z / a-z
        BIT            =  "0" / "1"
        CHAR           =  %x01-7F
        CR             =  %x0D
        CRLF           =  CR LF
        CTL            =  %x00-1F / %x7F
        DIGIT          =  %x30-39
        DQUOTE         =  %x22
        HEXDIG         =  DIGIT / "A" / "B" / "C" / "D" / "E" / "F"
        HTAB           =  %x09
        LF             =  %x0A
        LWSP           =  *(WSP / CRLF WSP)
        OCTET          =  %x00-FF
        SP             =  %x20
        VCHAR          =  %x21-7E
        WSP            =  SP / HTAB
|#

(defconstant +abnf-default-rules+
  `((:alpha  . (:char-class (:range #\A #\Z) (:range #\a #\z)))
    (:bit    . (:char-class #\0 #\1))
    (:char   . (:char-class (:range ,(code-char #x1) ,(code-char #x7f))))
    (:cr     . #\Newline)
    (:crlf   . (:sequence #\Newline #\Return))
    (:ctl    . (:char-class (:range ,(code-char #x0) ,(code-char #x1f))
	 		   ,(code-char #x7f)))
    (:digit  . (:char-class (:range #\0 #\9)))
    (:dquote . #\")
    (:hexdig . (:char-class (:range #\0 #\9) (:range #\A #\F)))
    (:htab   . #\Tab)
    (:lf     . #\Newline)
    (:lwsp   . (:regex "\s+"))
    (:octet  . (:char-class (:range ,(code-char #x0) ,(code-char #xff))))
    (:sp     . #\Space)
    (:vchar  . (:char-class (:range ,(code-char #x21) ,(code-char #x7e))))
    (:wsp    . (:char-class #\Space #\Tab)))
  "An alist of the usual rules needed for ABNF grammars")

(defun rule-name-character-p (character)
  (or (alphanumericp character)
      (char= character #\-)))

(defun vcharp (character)
  (<= #x21 (char-code character) #x7E))

(defrule vchar (+ (vcharp character))                   (:text t))
(defrule wsp (or #\Space #\Tab)                         (:constant :wsp))

(defrule comment (and ";" (* (or wsp vchar)) #\Newline) (:constant :comment))
(defrule c-nl (or comment #\Newline)                    (:constant :c-nl))
(defrule c-wsp (or wsp c-nl)                            (:constant :c-wsp))
(defrule n-wsp (* c-wsp)                                (:constant :c-wsp))

(defrule rule-name (and (alpha-char-p character)
			(+ (rule-name-character-p character)))
  (:lambda (name)
    (intern (string-upcase (text name)) :keyword)))

(defrule equal (and n-wsp #\= n-wsp)       (:constant :equal))
(defrule end-of-rule n-wsp                 (:constant :eor))

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

(defrule range-sep (or "-" ".")  (:constant :range-sep))

(defrule dec-range (and digits range-sep digits)
  (:lambda (range)
    (destructuring-bind (min sep max) range
      (declare (ignore sep))
      `(:char-class (:range ,(code-char min) ,(code-char max))))))

(defrule dec-val (and "d" (or dec-range digits))
  (:lambda (dv)
    (destructuring-bind (d val) dv
      (declare (ignore d))
      val)))))

(defun hexadecimal-char-p (character)
  (member character #. (quote (coerce "0123456789abcdef" 'list))))

(defrule hexdigits (+ (hexadecimal-char-p character))
  (:lambda (hx)
    (parse-integer (text hx) :radix 16)))

(defrule hex-range (and hexdigits range-sep hexdigits)
  (:lambda (range)
    (destructuring-bind (min sep max) range
      (declare (ignore sep))
      `(:char-class (:range ,(code-char min) ,(code-char max))))))

(defrule hex-val (and "x" (or hex-range digits))
  (:lambda (dv)
    (destructuring-bind (d val) dv
      (declare (ignore d))
      val)))))

(defrule num-val (and "%" (or dec-val hex-val))
  (:lambda (nv)
    (destructuring-bind (percent val) nv
      (declare (ignore percent))
      val)))

;;; allow to parse rule definitions without a separating blank line
(defrule rule-name-reference (and rule-name (! equal))
  (:lambda (ref)
    (destructuring-bind (rule-name nil) ref
      rule-name)))

(defrule element (or rule-name-reference char-val num-val))

(defrule repeat-var (and (? digits) "*" (? digits))
  (:lambda (rv)
    (destructuring-bind (min star max) rv
      (declare (ignore star))
      (cons (or min 0) max))))

(defrule repeat-specific digits
  (:lambda (digits)
    (cons digits digits)))

(defrule repeat (or repeat-var repeat-specific))

(defrule repetition (and (? repeat) toplevel-element)
  (:lambda (repetition)
    (destructuring-bind (repeat element) repetition
	(if repeat
	    (destructuring-bind (min . max) repeat
	      `(:non-greedy-repetition ,min ,max ,element))
	    ;; no repeat clause
	    element))))

(defrule concatenation-element (and n-wsp repetition)
  (:lambda (ce)
    (destructuring-bind (n-wsp rep) ce
      (declare (ignore n-wsp))
      rep)))

(defrule concatenation (and repetition (* concatenation-element))
  (:lambda (concat)
    (destructuring-bind (rep1 rlist) concat
      (if rlist
	  `(:sequence ,@(list* rep1 rlist))
	  ;; concatenation of a single element
	  rep1))))

(defrule alternation-element (and n-wsp "/" n-wsp concatenation)
  (:lambda (ae)
    (destructuring-bind (ws1 sl ws2 concatenation) ae
      (declare (ignore ws1 sl ws2))
      concatenation)))

(defrule alternation (and concatenation (* alternation-element))
  (:lambda (alternation)
    (destructuring-bind (c1 clist) alternation
      (if clist
	  `(:alternation ,@(list* c1 clist))
	  c1))))

(defrule group (and "(" n-wsp alternation n-wsp ")")
  (:lambda (group)
    (destructuring-bind (open ws1 a ws2 close) group
      (declare (ignore open close ws1 ws2))
      `(:group ,a))))

(defrule option (and "[" n-wsp alternation n-wsp "]")
  (:lambda (option)
    (destructuring-bind (open ws1 a ws2 close) option
      (declare (ignore open close ws1 ws2))
      `(:non-greedy-repetition 0 1 ,a))))

(defrule toplevel-element (or group option element))

(defrule elements (and (+ (and alternation n-wsp)) end-of-rule)
  (:lambda (elist)
    (destructuring-bind (elements eor) elist
      (declare (ignore eor))
      (concatenate 'list (mapcar #'car elements)))))

(defrule rule (and n-wsp rule-name equal elements)
  (:lambda (rule)
    (destructuring-bind (n-wsp rule-name eq definition) rule
      (declare (ignore n-wsp eq))
      (cons rule-name definition))))

(defrule rule-list (+ rule))

(defun parse-abnf-grammar (string &key junk-allowed)
  "Parse STRING as an ABNF grammar as defined in RFC 2234. Returns a regular
   expression that will only match strings conforming to given grammar.

   See http://tools.ietf.org/html/rfc2234 for details about the ABNF specs."

  (parse 'rule-list string :junk-allowed junk-allowed))

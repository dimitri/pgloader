;;;
;;; Augmented BNF for Syntax Specifications: ABNF
;;;
;;; Parsing ABNF syntaxes so that we can offer users to edit them
;;;
;;; see http://tools.ietf.org/html/draft-ietf-syslog-protocol-15#page-10
;;; and http://tools.ietf.org/html/rfc2234
;;;
(in-package #:abnf)

(defvar *abnf-rfc2234-abnf-definition*
  "     rulelist       =  1*( rule / (*c-wsp c-nl) )

        rule           =  rulename defined-as elements c-nl
                               ; continues if next line starts
                               ;  with white space

        rulename       =  ALPHA *(ALPHA / DIGIT / \"-\")

        defined-as     =  *c-wsp (\"=\" / \"=/\") *c-wsp
                               ; basic rules definition and
                               ;  incremental alternatives

        elements       =  alternation *c-wsp

        c-wsp          =  WSP / (c-nl WSP)

        c-nl           =  comment / CRLF
                               ; comment or newline

        comment        =  \";\" *(WSP / VCHAR) CRLF

        alternation    =  concatenation
                          *(*c-wsp \"/\" *c-wsp concatenation)

        concatenation  =  repetition *(1*c-wsp repetition)

        repetition     =  [repeat] element

        repeat         =  1*DIGIT / (*DIGIT \"*\" *DIGIT)

        element        =  rulename / group / option /
                          char-val / num-val / prose-val

        group          =  \"(\" *c-wsp alternation *c-wsp \")\"

        option         =  \"[\" *c-wsp alternation *c-wsp \"]\"

        char-val       =  DQUOTE *(%x20-21 / %x23-7E) DQUOTE
                               ; quoted string of SP and VCHAR without DQUOTE

        num-val        =  \"%\" (bin-val / dec-val / hex-val)

        bin-val        =  \"b\" 1*BIT
                          [ 1*(\".\" 1*BIT) / (\"-\" 1*BIT) ]
                               ; series of concatenated bit values
                               ; or single ONEOF range

        dec-val        =  \"d\" 1*DIGIT
                          [ 1*(\".\" 1*DIGIT) / (\"-\" 1*DIGIT) ]

        hex-val        =  \"x\" 1*HEXDIG
                          [ 1*(\".\" 1*HEXDIG) / (\"-\" 1*HEXDIG) ]

        prose-val      =  \"<\" *(%x20-3D / %x3F-7E) \">\"
                               ; bracketed string of SP and VCHAR without angles
                               ; prose description, to be used as last resort
  "
  "See http://tools.ietf.org/html/rfc2234#section-4")

(defvar *abnf-rfc-syslog-draft-15*
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

(defvar *abnf-rsyslog*
  (concatenate 'string
	       "RSYSLOG-MSG = \"<\" PRIVAL \">\" VERSION SP TIMESTAMP
                              SP HOSTNAME SP APP-NAME SP PROCID SP MSGID
                              SP [SD-ID SP] DATA

                DATA        = ~/.*/

                PRIVAL      = 1*3DIGIT ; range 0 .. 191"
	       '(#\Newline #\Newline)
	       *abnf-rfc-syslog-draft-15*)
  "See http://www.rsyslog.com/doc/syslog_protocol.html")

(defvar *abnf-rfc5424-syslog-protocol*
  "   SYSLOG-MSG      = HEADER SP STRUCTURED-DATA [SP MSG]

      HEADER          = PRI VERSION SP TIMESTAMP SP HOSTNAME
                        SP APP-NAME SP PROCID SP MSGID
      PRI             = \"<\" PRIVAL \">\"
      PRIVAL          = 1*3DIGIT ; range 0 .. 191
      VERSION         = NONZERO-DIGIT 0*2DIGIT
      HOSTNAME        = NILVALUE / 1*255PRINTUSASCII

      APP-NAME        = NILVALUE / 1*48PRINTUSASCII
      PROCID          = NILVALUE / 1*128PRINTUSASCII
      MSGID           = NILVALUE / 1*32PRINTUSASCII

      TIMESTAMP       = NILVALUE / FULL-DATE \"T\" FULL-TIME
      FULL-DATE       = DATE-FULLYEAR \"-\" DATE-MONTH \"-\" DATE-MDAY
      DATE-FULLYEAR   = 4DIGIT
      DATE-MONTH      = 2DIGIT  ; 01-12
      DATE-MDAY       = 2DIGIT  ; 01-28, 01-29, 01-30, 01-31 based on
                                ; month/year
      FULL-TIME       = PARTIAL-TIME TIME-OFFSET
      PARTIAL-TIME    = TIME-HOUR \":\" TIME-MINUTE \":\" TIME-SECOND
                        [TIME-SECFRAC]
      TIME-HOUR       = 2DIGIT  ; 00-23
      TIME-MINUTE     = 2DIGIT  ; 00-59
      TIME-SECOND     = 2DIGIT  ; 00-59
      TIME-SECFRAC    = \".\" 1*6DIGIT
      TIME-OFFSET     = \"Z\" / TIME-NUMOFFSET
      TIME-NUMOFFSET  = (\"+\" / \"-\") TIME-HOUR \":\" TIME-MINUTE


      STRUCTURED-DATA = NILVALUE / 1*SD-ELEMENT
      SD-ELEMENT      = \"[\" SD-ID *(SP SD-PARAM) \"]\"
      SD-PARAM        = PARAM-NAME \"=\" %d34 PARAM-VALUE %d34
      SD-ID           = SD-NAME
      PARAM-NAME      = SD-NAME
      PARAM-VALUE     = UTF-8-STRING ; characters '\"', '\' and
                                     ; ']' MUST be escaped.
      SD-NAME         = 1*32PRINTUSASCII
                        ; except '=', SP, ']', %d34 (\")

      MSG             = MSG-ANY / MSG-UTF8
      MSG-ANY         = *OCTET ; not starting with BOM
      MSG-UTF8        = BOM UTF-8-STRING
      BOM             = %xEF.BB.BF

      UTF-8-STRING    = *OCTET ; UTF-8 string as specified
                        ; in RFC 3629

      OCTET           = %d00-255
      SP              = %d32
      PRINTUSASCII    = %d33-126
      NONZERO-DIGIT   = %d49-57
      DIGIT           = %d48 / NONZERO-DIGIT
      NILVALUE        = \"-\""
  "See http://tools.ietf.org/html/rfc5424#section-6")

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

(defvar *abnf-default-rules*
  `((:abnf-alpha  (:char-class (:range #\A #\Z) (:range #\a #\z)))
    (:abnf-bit    (:char-class #\0 #\1))
    (:abnf-char   (:char-class (:range ,(code-char #x1) ,(code-char #x7f))))
    (:abnf-cr     #\Newline)
    (:abnf-crlf   (:sequence #\Newline #\Return))
    (:abnf-ctl    (:char-class (:range ,(code-char #x0) ,(code-char #x1f))
			  ,(code-char #x7f)))
    (:abnf-digit  (:char-class (:range #\0 #\9)))
    (:abnf-dquote #\")
    (:abnf-hexdig (:char-class (:range #\0 #\9) (:range #\A #\F)))
    (:abnf-htab   #\Tab)
    (:abnf-lf     #\Newline)
    (:abnf-lwsp   (:regex "\s+"))
    (:abnf-octet  (:char-class (:range ,(code-char #x0) ,(code-char #xff))))
    (:abnf-sp     #\Space)
    (:abnf-vchar  (:char-class (:range ,(code-char #x21) ,(code-char #x7e))))
    (:abnf-wsp    (:char-class #\Space #\Tab)))
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

(defun rule-name-symbol (rule-name &key find-symbol)
  "Turn the string we read in the ABNF into internal symbol."
  (let ((symbol-fun  (if find-symbol #'find-symbol #'intern))
	(symbol-name (string-upcase (format nil "abnf-~a" rule-name))))
    (funcall symbol-fun symbol-name :keyword)))

(defrule rule-name (and (alpha-char-p character)
			(+ (rule-name-character-p character)))
  (:lambda (name)
    (rule-name-symbol (text name))))

(defrule equal (and n-wsp #\= n-wsp)       (:constant :equal))
(defrule end-of-rule n-wsp                 (:constant :eor))

(defrule digit (digit-char-p character)
  (:lambda (digit)
    (parse-integer (text digit))))

(defrule digits (+ (digit-char-p character))
  (:lambda (digits)
    (code-char (parse-integer (text digits)))))

(defun char-val-char-p (character)
  (let ((code (char-code character)))
    (or (<= #x20 code #x21)
	(<= #x23 code #x7E))))

(defrule char-val (and #\" (* (char-val-char-p character)) #\")
  (:lambda (char)
    (destructuring-bind (open val close) char
      (declare (ignore open close))
      (text val))))

(defrule dec-string (and digits (+ (and "." digits)))
  (:lambda (string)
    (destructuring-bind (first rest) string
      `(:sequence ,first ,@(mapcar #'cadr rest)))))

(defrule dec-range (and digits "-" digits)
  (:lambda (range)
    (destructuring-bind (min sep max) range
      (declare (ignore sep))
      `(:char-class (:range ,min ,max)))))

(defrule dec-val (and "d" (or dec-string dec-range digits))
  (:lambda (dv)
    (destructuring-bind (d val) dv
      (declare (ignore d))
      val)))

(defun hexadecimal-char-p (character)
  (member character #. (quote (coerce "0123456789abcdefABCDEF" 'list))))

(defrule hexdigits (+ (hexadecimal-char-p character))
  (:lambda (hx)
    (code-char (parse-integer (text hx) :radix 16))))

(defrule hex-string (and hexdigits (+ (and "." hexdigits)))
  (:lambda (string)
    (destructuring-bind (first rest) string
      `(:sequence ,first ,@(mapcar #'cadr rest)))))

(defrule hex-range (and hexdigits range-sep hexdigits)
  (:lambda (range)
    (destructuring-bind (min sep max) range
      (declare (ignore sep))
      `(:char-class (:range ,min ,max)))))

(defrule hex-val (and "x" (or hex-string hex-range hexdigits))
  (:lambda (dv)
    (destructuring-bind (d val) dv
      (declare (ignore d))
      val)))

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

;;; what about allowing regular expressions directly?
(defun process-quoted-regex (pr)
  "Helper function to process different kinds of quotes for regexps"
  (destructuring-bind (open regex close) pr
      (declare (ignore open close))
      `(:regex ,(text regex))))

(defrule single-quoted-regex (and #\' (+ (not #\')) #\')
  (:function process-quoted-regex))

(defrule double-quoted-regex (and #\" (+ (not #\")) #\")
  (:function process-quoted-regex))

(defrule parens-quoted-regex (and #\( (+ (not #\))) #\))
  (:function process-quoted-regex))

(defrule braces-quoted-regex (and #\{ (+ (not #\})) #\})
  (:function process-quoted-regex))

(defrule chevron-quoted-regex (and #\< (+ (not #\>)) #\>)
  (:function process-quoted-regex))

(defrule brackets-quoted-regex (and #\[ (+ (not #\])) #\])
  (:function process-quoted-regex))

(defrule slash-quoted-regex (and #\/ (+ (not #\/)) #\/)
  (:function process-quoted-regex))

(defrule pipe-quoted-regex (and #\| (+ (not #\|)) #\|)
  (:function process-quoted-regex))

(defrule sharp-quoted-regex (and #\# (+ (not #\#)) #\#)
  (:function process-quoted-regex))

(defrule quoted-regex (and "~" (or single-quoted-regex
				   double-quoted-regex
				   parens-quoted-regex
				   braces-quoted-regex
				   chevron-quoted-regex
				   brackets-quoted-regex
				   slash-quoted-regex
				   pipe-quoted-regex
				   sharp-quoted-regex))
  (:lambda (qr)
    (destructuring-bind (tilde regex) qr
      (declare (ignore tilde))
      regex)))

(defrule element (or rule-name-reference char-val num-val quoted-regex))

(defrule number (+ (digit-char-p character))
  (:lambda (number)
    (parse-integer (text number))))

(defrule repeat-var (and (? number) "*" (? number))
  (:lambda (rv)
    (destructuring-bind (min star max) rv
      (declare (ignore star))
      (cons (or min 0) max))))

(defrule repeat-specific number
  (:lambda (number)
    (cons number number)))

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
      ;; we need the grouping when parsing the ABNF syntax, but once this
      ;; parsing is done there's no ambiguity possible left and we don't
      ;; need the grouping anymore in the resulting regular-expression parse
      ;; tree.
      a)))

(defrule option (and "[" n-wsp alternation n-wsp "]")
  (:lambda (option)
    (destructuring-bind (open ws1 a ws2 close) option
      (declare (ignore open close ws1 ws2))
      `(:non-greedy-repetition 0 1 ,a))))

(defrule toplevel-element (or group option element))

(defrule alternations (and n-wsp alternation)
  (:lambda (alts)
    (destructuring-bind (n-wsp alt) alts
      (declare (ignore n-wsp))
      alt)))

(defrule elements (and (+ alternations) end-of-rule)
  (:lambda (alist)
    (destructuring-bind (alts eor) alist
      (declare (ignore eor))
      alts)))

(defrule rule (and n-wsp rule-name equal elements)
  (:lambda (rule)
    (destructuring-bind (n-wsp rule-name eq definition) rule
      (declare (ignore n-wsp eq))
      (cons rule-name definition))))

(defrule rule-list (+ rule))

;;;
;;; Now that we are able to transform ABNF rule set into an alist of
;;; cl-ppcre parse trees and references to other rules in the set, we need
;;; to expand each symbol's definition to get a real cl-ppcre scanner parse
;;; tree.
;;;
(defun expand-rule-definition (definition
			       rule-set
			       registering-rules
			       already-expanded-rules)
  "Expand given rule DEFINITION within given RULE-SET"
  (typecase definition
    (list
     ;; walk the definition and expand its elements
     (loop
	for element in definition
	collect (expand-rule-definition element
					rule-set
					registering-rules
					already-expanded-rules)))

    (symbol
     (if (member definition '(:sequence
			      :alternation
			      :regex
			      :char-class
			      :range
			      :non-greedy-repetition))
	 ;; that's a cl-ppcre scanner parse-tree symbol
	 ;; only put in that list those cl-ppcre symbols we actually produce
	 definition

	 ;; here we have to actually expand the symbol
	 (progn
	   ;; first protect against infinite recursion
	   (when (member definition already-expanded-rules)
	     (error "Can not expand recursive rule: ~S." definition))

	   (destructuring-bind (rule-name rule-definition)
	       (or (assoc definition rule-set)
		   (assoc definition *abnf-default-rules*))
	     (let* ((already-expanded-rules
		     (cons definition already-expanded-rules))

		    (expanded-definition
		     (expand-rule-definition rule-definition
					     rule-set
					     registering-rules
					     already-expanded-rules)))
	       (if (member rule-name registering-rules)
		   `(:register ,expanded-definition)
		   expanded-definition))))))

    ;; all other types of data are "constants" in our parse-tree
    (t definition)))

(defun expand-rule (rule-name rule-set &optional registering-rules)
  "Given a rule, expand it completely removing references to other parsed
   rules"
  (let ((rule (rule-name-symbol rule-name :find-symbol t)))
    (destructuring-bind (rule-name definition)
	   (assoc rule rule-set)
      `(:sequence
	:start-anchor
	,(expand-rule-definition definition
				rule-set
				(loop
				   for rr in registering-rules
				   collect (rule-name-symbol rr :find-symbol t))
				(list rule-name))))))

(defun parse-abnf-grammar (abnf-string top-level-rule
			   &key registering-rules junk-allowed)
  "Parse STRING as an ABNF grammar as defined in RFC 2234. Returns a cl-ppcre
   scanner that will only match strings conforming to given grammar.

   See http://tools.ietf.org/html/rfc2234 for details about the ABNF specs.
   Added to that grammar is support for regular expression, that are
   expected in the ELEMENT production and spelled ~/regex/. The allowed
   delimiters are: ~// ~[] ~{} ~() ~<> ~\"\" ~'' ~|| and ~##."
  (let ((rule-set
	 (parse 'rule-list abnf-string :junk-allowed junk-allowed)))
    (cl-ppcre:create-scanner
     (expand-rule top-level-rule
		  ;; in case of duplicates only keep the latest addition
		  (remove-duplicates rule-set :key #'car)
		  registering-rules))))

(defun test (&key (times 10000))
  "This serves as a test and an example: if you're going to use the same
   scanner more than one, be sure to compute it only once."
  (let* ((cl-ppcre:*use-bmh-matchers* t)
	 (cl-ppcre:*optimize-char-classes* t)
	 (scanner
	  (parse-abnf-grammar *abnf-rfc-syslog-draft-15*
			      :timestamp
			      :registering-rules '(:full-date
						   :partial-time
						   :time-offset))))
   (loop
      repeat times
      do (cl-ppcre:register-groups-bind
	      (date time zone)
	    (scanner "2013-09-08T00:02:03.123456Z+02:00")
	  (list date time zone)))))

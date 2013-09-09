# ABNF DEFINITION OF ABNF

This Common Lisp librairie implements a parser generator for the ABNF
grammar format as described in [http://tools.ietf.org/html/rfc2234](RFC
2234).

The generated parser is a regular expression scanner provided by the
[http://weitz.de/cl-ppcre/](cl-ppcre) lib, which means that we can't parse
recursive grammar definition. One such definition is the ABNF definition as
given by the RFC. Fortunately, as you have this lib, you most probably don't
need to generate another parser to handle that particular ABNF grammar.

## Installation

The system has been made Quicklisp ready.

    $ cd ~/quicklisp/local-projects/
	$ git clone git://git.tapoueh.org/pgloader.git
	* (ql:quickload "abnf")

Currently the ABNF system is maintained as part of the `pgloader` tool as a
central piece of its syslog message parser facility.

## Usage

The `parse-abnf-grammar` function expects the grammar to be parsed as a
string, and also needs the top level rule name of the grammar you're
interested into, as a symbol or a string. You can also give a list of rule
names that you want to capture, they will be capture in the order in which
they are needed to expand the given top-level rule.

The `parse-abnf-grammar` function returns a `cl-ppcre` scanner.

~~~ {#example.lisp .commonlisp .numberLines}
(defvar *timestamp-abnf*
  "   TIMESTAMP       = NILVALUE / FULL-DATE \"T\" FULL-TIME
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

      NILVALUE        = \"-\" "
  "A timestamp ABNF grammar.")

(let ((scanner (abnf:parse-abnf-grammar *timestamp-abnf*
					:timestamp
					:registering-rules '(:full-date))))
  (cl-ppcre:register-groups-bind (date)
      (scanner "2013-09-08T00:02:03.123456Z+02:00")
    date))
~~~

In the previous usage example the `let` block returns `"2013-09-08"`.

## ABNF grammar

This library supports the ABNF grammar as given in RFC 2234, with additional
support for plain regular expressions.

### Parsed grammar

Here's the RFC syntax:

        rulelist       =  1*( rule / (*c-wsp c-nl) )

        rule           =  rulename defined-as elements c-nl
                               ; continues if next line starts
                               ;  with white space

        rulename       =  ALPHA *(ALPHA / DIGIT / "-")

        defined-as     =  *c-wsp ("=" / "=/") *c-wsp
                               ; basic rules definition and
                               ;  incremental alternatives

        elements       =  alternation *c-wsp

        c-wsp          =  WSP / (c-nl WSP)

        c-nl           =  comment / CRLF
                               ; comment or newline

        comment        =  ";" *(WSP / VCHAR) CRLF

        alternation    =  concatenation
                          *(*c-wsp "/" *c-wsp concatenation)

        concatenation  =  repetition *(1*c-wsp repetition)

        repetition     =  [repeat] element

        repeat         =  1*DIGIT / (*DIGIT "*" *DIGIT)

        element        =  rulename / group / option /
                          char-val / num-val / prose-val / regex
                               ; regex is an addition of this lib, see above

        group          =  "(" *c-wsp alternation *c-wsp ")"

        option         =  "[" *c-wsp alternation *c-wsp "]"

        char-val       =  DQUOTE *(%x20-21 / %x23-7E) DQUOTE
                               ; quoted string of SP and VCHAR
                               ;   without DQUOTE

        num-val        =  "%" (bin-val / dec-val / hex-val)

        bin-val        =  "b" 1*BIT
                          [ 1*("." 1*BIT) / ("-" 1*BIT) ]
                               ; series of concatenated bit values
                               ; or single ONEOF range

        dec-val        =  "d" 1*DIGIT
                          [ 1*("." 1*DIGIT) / ("-" 1*DIGIT) ]

        hex-val        =  "x" 1*HEXDIG
                          [ 1*("." 1*HEXDIG) / ("-" 1*HEXDIG) ]

        prose-val      =  "<" *(%x20-3D / %x3F-7E) ">"
                               ; bracketed string of SP and VCHAR
                               ;   without angles
                               ; prose description, to be used as
                               ;   last resort

### Core rules

Those parts of the grammar are always provided, they are the *defaults*
rules of the ABNF definition.

        ALPHA          =  %x41-5A / %x61-7A   ; A-Z / a-z

        BIT            =  "0" / "1"

        CHAR           =  %x01-7F
                               ; any 7-bit US-ASCII character, excluding NUL

        CR             =  %x0D
                               ; carriage return

        CRLF           =  CR LF
                               ; Internet standard newline

        CTL            =  %x00-1F / %x7F
                               ; controls

        DIGIT          =  %x30-39
                               ; 0-9

        DQUOTE         =  %x22
                               ; " (Double Quote)

        HEXDIG         =  DIGIT / "A" / "B" / "C" / "D" / "E" / "F"

        HTAB           =  %x09
                               ; horizontal tab

        LF             =  %x0A
                               ; linefeed

        LWSP           =  *(WSP / CRLF WSP)
                               ; linear white space (past newline)

        OCTET          =  %x00-FF
                               ; 8 bits of data

        SP             =  %x20

### Regex Support

We add support for plain regexp in the `element` rule. A regexp is expected
to follow the form:

       regex           = "~" delimiter expression delimiter

The *expression* shouldn't contain the *delimiter* of course, and the
allowed delimiters are `~//`, `~[]`, `~{}`, `~()`, `~<>`, `~""`, `~''`,
`~||` and `~##`. If you have to build a regexp with more than one of those
delimiters in it, you can just concatenate multiple parts together like in
this example:

     complex-regex  = ~/foo{bar}/ ~{baz/quux}

That will be used in exactly the same way as the following example:

     complex-regex  = ~<foo{bar}baz/quux>



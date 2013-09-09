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
  (register-groups-bind (date)
      (scanner "2013-09-08T00:02:03.123456Z+02:00")
    date))
~~~

In the previous usage example the `let` block returns `"2013-09-08"`.

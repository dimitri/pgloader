;;;; abnf-example.lisp

(defpackage #:abnf-example
  (:use #:cl #:abnf #:cl-ppcre))

(in-package #:abnf-example)

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

(let ((scanner (parse-abnf-grammar *timestamp-abnf*
				   :timestamp
				   :registering-rules '(:full-date))))
  (register-groups-bind (date)
      (scanner "2013-09-08T00:02:03.123456Z+02:00")
    date))

(let ((scanner (parse-abnf-grammar *timestamp-abnf*
				   :timestamp
				   :registering-rules '(:full-date))))
  (scan-to-strings scanner "2013-09-08T00:02:03.123456Z+02:00"))

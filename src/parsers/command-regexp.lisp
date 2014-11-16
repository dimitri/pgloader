;;;
;;; Parse the pgloader commands grammar
;;;

(in-package :pgloader.parser)

;;;
;;; Regular Expression support, quoted as-you-like
;;;
(defun process-quoted-regex (pr)
  "Helper function to process different kinds of quotes for regexps"
  (bind (((_ regex _) pr))
    (list :regex (text regex))))

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
    (bind (((_ regex) qr)) regex)))

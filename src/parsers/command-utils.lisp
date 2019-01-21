;;;
;;; Parse the pgloader commands grammar
;;;

(in-package :pgloader.parser)

(defvar *cwd* nil
  "Parser Current Working Directory")

(defvar *data-expected-inline* nil
  "Set to :inline when parsing an INLINE keyword in a FROM clause.")

;;
;; Some useful rules
;;
(defrule single-line-comment (and "--" (* (not #\Newline)) #\Newline)
  (:constant :comment))

(defrule multi-line-comment (and "/*" (+ (not "*/")) "*/")
  (:constant :comment))

(defrule comments (or single-line-comment multi-line-comment))

(defrule keep-a-single-whitespace (+ (or #\space #\tab #\newline #\linefeed))
  (:constant " "))

(defrule whitespace (+ (or #\space #\tab #\return #\newline #\linefeed comments))
  (:constant 'whitespace))

(defrule ignore-whitespace (* whitespace)
  (:constant nil))

(defrule punct (or #\- #\_ #\$ #\%)
  (:text t))

(defrule namestring (and (or #\_ (alpha-char-p character))
			 (* (or (alpha-char-p character)
				(digit-char-p character)
				punct)))
  (:text t))

(defrule double-quoted-namestring (and #\" (* (not #\")) #\")
  (:lambda (dqn) (text (second dqn))))

(defrule quoted-namestring (and #\' (+ (not #\')) #\')
  (:lambda (dqn) (text (second dqn))))

(defrule name (or namestring quoted-namestring)
  (:text t))

(defrule trimmed-name (and ignore-whitespace name)
  (:destructure (whitespace name) (declare (ignore whitespace)) name))

(defrule namestring-or-regex (or quoted-namestring quoted-regex))

(defrule maybe-quoted-namestring (or double-quoted-namestring
                                     quoted-namestring
                                     namestring))

(defrule open-paren (and ignore-whitespace #\( ignore-whitespace)
  (:constant :open-paren))

(defrule close-paren (and ignore-whitespace #\) ignore-whitespace)
  (:constant :close-paren))

(defrule comma-separator (and ignore-whitespace #\, ignore-whitespace)
  (:constant ","))

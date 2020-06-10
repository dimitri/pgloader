;;;
;;; Parse user given s-expressions in commands
;;;

(in-package #:pgloader.parser)

(defun not-doublequote (char)
  (not (eql #\" char)))

(defun symbol-character-p (character)
  (not (member character '(#\Space #\( #\)))))

(defun symbol-first-character-p (character)
  (and (symbol-character-p character)
       (not (member character '(#\+ #\-)))))

(defrule sexp-symbol (and (symbol-first-character-p character)
			  (* (symbol-character-p character)))
  (:lambda (schars)
    (if (char= #\: (car schars))
        (read-from-string (text schars))
        (pgloader.transforms:intern-symbol
         (text schars)
         '(("nil"       . cl:nil)
           ("cl:t"      . cl:t)
           ("precision" . pgloader.transforms::precision)
           ("scale"     . pgloader.transforms::scale)
           ("split-sequence" . split-sequence:split-sequence))))))

(defrule sexp-char (and #\# #\\
                        (alpha-char-p character)
                        (+ (or (alpha-char-p character)
                               (digit-char-p character)
                               #\_)))
  (:lambda (char-name)
    (read-from-string (text char-name))))

(defrule sexp-string-char (or (not-doublequote character) (and #\\ #\")))

(defrule sexp-string (and #\" (* sexp-string-char) #\")
  (:destructure (q1 string q2)
    (declare (ignore q1 q2))
    (text string)))

(defrule sexp-integer (+ (or "0" "1" "2" "3" "4" "5" "6" "7" "8" "9"))
  (:lambda (list)
    (parse-integer (text list) :radix 10)))

(defrule sexp-list (and open-paren sexp (* sexp) close-paren)
  (:destructure (open car cdr close)
    (declare (ignore open close))
    (cons car cdr)))

(defrule sexp-atom (and ignore-whitespace
			(or sexp-char sexp-string sexp-integer sexp-symbol))
  (:lambda (atom)
    (bind (((_ a) atom)) a)))

(defrule sexp (or sexp-atom sexp-list))


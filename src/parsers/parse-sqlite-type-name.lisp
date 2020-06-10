;;;
;;; SQLite Type Names might be complex enough to warrant a full blown
;;; parsing activity.
;;;

(in-package :pgloader.parser)

(defrule extra-qualifiers (and (? " ")
                               (or (~ "unsigned")
                                   (~ "short")
                                   (~ "varying")
                                   (~ "native")
                                   (~ "nocase")
                                   (~ "auto_increment"))
                               (? " "))
  (:lambda (noise) (second noise)))

(defrule sqlite-single-typemod (and open-paren
                                    (+ (digit-char-p character))
                                    close-paren)
  (:lambda (st) (cons (parse-integer (text (second st))) nil)))

(defrule sqlite-double-typemod (and open-paren
                                    (+ (digit-char-p character))
                                    comma-separator
                                    (+ (digit-char-p character))
                                    close-paren)
  (:lambda (dt) (cons (parse-integer (text (second dt)))
                      (parse-integer (text (fourth dt))))))

(defrule sqlite-typemod (or sqlite-double-typemod sqlite-single-typemod))

(defrule sqlite-type-name (and (* extra-qualifiers)
                               (+ (or (alpha-char-p character) #\_))
                               (* extra-qualifiers)
                               ignore-whitespace
                               (? sqlite-typemod)
                               ignore-whitespace
                               (* extra-qualifiers))
  (:lambda (tn) (list (text (second tn))
                      (fifth tn)
                      (remove-if #'null
                                 (append (first tn) (third tn) (seventh tn))))))

(defun parse-sqlite-type-name (type-name)
  (if (string= type-name "")
      ;; yes SQLite allows for empty type names
      "text"
      (values-list (parse 'sqlite-type-name (string-downcase type-name)))))

;;;
;;; SQLite Type Names might be complex enough to warrant a full blown
;;; parsing activity.
;;;

(in-package :pgloader.parser)

(defrule extra-qualifiers (and (? " ")
                               (or (~ "unsigned")
                                   (~ "signed")
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

;; type names may include digits (int4, int8, float4, float8) and
;; multi-word forms ("double precision").
(defrule sqlite-type-name (+ (and (not extra-qualifiers)
                                  (+ (or (alpha-char-p character)
                                         (digit-char-p character)
                                         #\_))
                                  (? " ")))
  (:text t))

;; Optional trailing [] for ORM-generated array-like blob types (byte[], blob[]).
;; We preserve the brackets in the returned type string so that cast rules can
;; match on the exact name "byte[]" if desired (#1231).
(defrule sqlite-array-suffix (and #\[ #\])
  (:constant "[]"))

(defrule sqlite-type-expr (and (* extra-qualifiers)
                               sqlite-type-name
                               (? sqlite-array-suffix)
                               (* extra-qualifiers)
                               ignore-whitespace
                               (? sqlite-typemod)
                               ignore-whitespace
                               (* extra-qualifiers))
  (:lambda (tn)
    (let* ((base   (string-right-trim " " (text (second tn))))
           (suffix (or (third tn) ""))
           (name   (concatenate 'string base suffix)))
      (list name
            (sixth tn)
            (remove-if #'null
                       (append (first tn) (fourth tn) (eighth tn)))))))

(defun parse-sqlite-type-name (type-name)
  (if (string= type-name "")
      ;; yes SQLite allows for empty type names
      "text"
      (handler-case
          (values-list (parse 'sqlite-type-expr (string-downcase type-name)))
        ;; If the type name cannot be parsed at all (e.g. starts with a digit
        ;; or contains characters we don't handle), fall back to "text" and let
        ;; the catch-all cast rule deal with it.
        (esrap:esrap-parse-error ()
          (values "text" nil nil)))))

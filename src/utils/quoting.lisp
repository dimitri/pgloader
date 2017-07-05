;;;
;;; Manage PostgreSQL specific quoting of identifiers.
;;;
;;; We use this facility as early as possible (in schema-structs), so we
;;; need those bits of code in utils/ rather than pgsql/.
;;;

(in-package :pgloader.quoting)

(defun quoted-p (s)
  "Return true if s is a double-quoted string"
  (and (eq (char s 0) #\")
       (eq (char s (- (length s) 1)) #\")))

(defun apply-identifier-case (identifier)
  "Return given IDENTIFIER with CASE handled to be PostgreSQL compatible."
  (let* ((lowercase-identifier (cl-ppcre:regex-replace-all
                                "[^a-zA-Z0-9.]" (string-downcase identifier) "_"))
         (*identifier-case*
          ;; we might need to force to :quote in some cases
          ;;
          ;; http://www.postgresql.org/docs/9.1/static/sql-syntax-lexical.html
          ;;
          ;; SQL identifiers and key words must begin with a letter (a-z, but
          ;; also letters with diacritical marks and non-Latin letters) or an
          ;; underscore (_).
          (cond ((quoted-p identifier)
                 :none)

                ((not (cl-ppcre:scan "^[A-Za-z_][A-Za-z0-9_$]*$" identifier))
                 :quote)

                ((member lowercase-identifier *pgsql-reserved-keywords*
                         :test #'string=)
                 (progn
                   ;; we need to both downcase and quote here
                   (when (eq :downcase *identifier-case*)
                     (setf identifier lowercase-identifier))
                   :quote))

                ;; in other cases follow user directive
                (t *identifier-case*))))

    (ecase *identifier-case*
      (:downcase lowercase-identifier)
      (:quote    (format nil "~s"
                         (cl-ppcre:regex-replace-all "\"" identifier "\"\"")))
      (:none     identifier))))

(defun ensure-unquoted (identifier)
  (cond ((quoted-p identifier)
         ;; when the table name comes from the user (e.g. in the
         ;; load file) then we might have to unquote it: the
         ;; PostgreSQL catalogs does not store object names in
         ;; their quoted form.
         (subseq identifier 1 (1- (length identifier))))

        (t identifier)))

(defun build-identifier (sep &rest parts)
  "Concatenante PARTS into a PostgreSQL identifier, with SEP in between
   parts. That's useful for creating an index name from a table's oid and name."
  (apply-identifier-case
   (apply #'concatenate
          'string
          (loop :for (part . more?) :on parts
             :collect (ensure-unquoted (typecase part
                                         (string part)
                                         (t      (princ-to-string part))))
             :when more? :collect sep))))

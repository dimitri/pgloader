;;;
;;; Source parsing
;;;
;;; Source is either a local filename, stdin, a MySQL connection with a
;;; table-name, or an http uri.
;;;

(in-package :pgloader.parser)

;; parsing filename
(defun filename-character-p (char)
  (or (member char #.(quote (coerce "/\\:.-_!@#$%^&*()" 'list)))
      (alphanumericp char)))

(defrule stdin (~ "stdin") (:constant (list :stdin nil)))
(defrule inline (~ "inline")
  (:lambda (i)
    (declare (ignore i))
    (setf *data-expected-inline* :inline)
    (list :inline nil)))

(defrule filename (* (filename-character-p character))
  (:lambda (f)
    (list :filename (parse-namestring (coerce f 'string)))))

(defrule quoted-filename (and #\' (+ (not #\')) #\')
  (:lambda (q-f)
    (bind (((_ f _) q-f))
      (list :filename (parse-namestring (coerce f 'string))))))

(defrule maybe-quoted-filename (or quoted-filename filename)
  (:identity t))

(defrule http-uri (and "http://" (* (filename-character-p character)))
  (:destructure (prefix url)
    (list :http (concatenate 'string prefix url))))

(defrule maybe-quoted-filename-or-http-uri (or http-uri maybe-quoted-filename))

(defrule get-filename-or-http-uri-from-environment-variable (and kw-getenv name)
  (:lambda (p-e-v)
    (destructuring-bind (g varname) p-e-v
      (declare (ignore g))
      (let ((connstring (getenv-default varname)))
        (unless connstring
          (error "Environment variable ~s is unset." varname))
        (parse 'maybe-quoted-filename-or-http-uri connstring)))))

(defrule filename-or-http-uri (or get-filename-or-http-uri-from-environment-variable
                                  maybe-quoted-filename-or-http-uri))

(defrule source-uri (or stdin
			http-uri
			db-connection-uri
			maybe-quoted-filename)
  (:identity t))

(defrule load-from (and (~ "LOAD") ignore-whitespace (~ "FROM"))
  (:constant :load-from))

(defrule source (and load-from ignore-whitespace source-uri)
  (:destructure (load-from ws source)
    (declare (ignore load-from ws))
    source))

(defrule database-source (and kw-load kw-database kw-from
			      (or db-connection-uri
                                  get-dburi-from-environment-variable))
  (:lambda (source)
    (bind (((_ _ _ uri) source)) uri)))

;;;
;;; Parse the pgloader commands grammar
;;;

(in-package :pgloader.parser)

(defparameter *default-postgresql-port* 5432)

;;
;; Some useful rules
;;
(defrule whitespace (+ (or #\space #\tab #\newline))
  (:constant 'whitespace))

(defrule ignore-whitespace (* whitespace)
  (:constant nil))

(defrule punct (or #\, #\- #\_)
  (:text t))

(defrule namestring (and (alpha-char-p character)
			 (* (or (alpha-char-p character)
				(digit-char-p character)
				punct)))
  (:text t)))

(defrule quoted-namestring (and #\' namestring #\')
  (:destructure (open name close) (declare (ignore open close)) name))

(defrule name (or namestring quoted-namestring)
  (:text t))

(defrule trimmed-name (and ignore-whitespace name)
  (:destructure (whitespace name) (declare (ignore whitespace)) name))))

;;
;; Parse PostgreSQL database connection strings
;;
;;  at postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
;;
;; http://www.postgresql.org/docs/9.2/static/libpq-connect.html#LIBPQ-CONNSTRING
;;
(defrule dsn-port (and ":" (* (digit-char-p character)))
  (:destructure (colon digits &aux (port (coerce digits 'string)))
		(declare (ignore colon))
		(list :port (if (null digits)
				*default-postgresql-port*
				(parse-integer port)))))

(defrule dsn-user-password (and namestring
				(? (and ":" (? namestring)))
				"@")
  (:lambda (args)
    (destructuring-bind (username &optional password)
	(butlast args)
      ;; password looks like '(":" "password")
      (list :user username :password (cadr password)))))

(defrule hostname (and namestring (? (and "." hostname)))
  (:text t))

(defrule dsn-hostname (and hostname (? dsn-port))
  (:destructure (hostname &optional port)
		(append (list :host hostname)
			(or port
			    (list :port *default-postgresql-port*)))))

(defrule dsn-dbname (and "/" namestring)
  (:destructure (slash dbname)
		(declare (ignore slash))
		(list :dbname dbname)))

(defrule postgresql-connection-uri (and "postgresql://"
					(? dsn-user-password)
					(? dsn-hostname)
					dsn-dbname)
  (:lambda (uri)
    (destructuring-bind (&key user
			      password
			      (host "localhost")
			      (port 5432)
			      dbname)
	;; ignore the postgresql:// prefix, (first uri)
	(append (second uri) (third uri) (fourth uri))
      (list :user user
	    :password password
	    :host host
	    :port port
	    :dbname dbname))))

(defrule target-dsn (and "at" ignore-whitespace connection-uri)
  (:destructure (at whitespace uri) (declare (ignore at whitespace)) uri))

;;
;; The main target parsing
;;
;;  COPY target-table-name AT connection-uri
;;  COPY foo AT postgresql://user@localhost:5432/dbname
;;
(defrule target (and "COPY" trimmed-name (? (and ignore-whitespace target-dsn)))
  (:destructure (copy target &optional dsn)
    (declare (ignore copy))
    (append (list :table-name target) (cadr dsn))))

;;
;; Source parsing (filename)
;;

;; parsing filename
(defun filename-character-p (char)
  (or (member char #.(quote (coerce "/\\:.-_!@#$%^&*() " 'list)))
      (alphanumericp char)))

(defrule filename (and #\'
		       (* (filename-character-p character))
		       #\')
  (:destructure (open f close)
    (declare (ignore open close))
    (parse-namestring (coerce f 'string))))

(defrule trimmed-filename (and ignore-whitespace filename)
  (:destructure (whitespace filename) (declare (ignore whitespace)) filename))

(defrule source (and "FROM" trimmed-filename)
  (:destructure (from source)
    (declare (ignore from))
    source))

;;
;; Putting it all together, the COPY command
;;
;; The output format is Lisp code using the pgloader API.
;;
(defrule copy (and target ignore-whitespace source)
  (:destructure (target whitespace source)
    (declare (ignore whitespace))
    (destructuring-bind (&key table-name user password host port dbname)
	target
      `(lambda (&key (*pgconn-host* ,host)
		  (*pgconn-port* ,port)
		  (*pgconn-user* ,user)
		  (*pgconn-pass* ,password))
	 (pgloader.pgsql:copy-from-file ,dbname ,table-name ,source)))))

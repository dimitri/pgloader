;;;
;;; Parse the pgloader commands grammar
;;;

(in-package :pgloader.parser)

(defparameter *default-postgresql-port* 5432)
(defparameter *default-mysql-port* 3306)

#|
Here's a quick description of the format we're parsing here:

    LOAD FROM '/path/to/filename.txt'
               stdin
               http://url.to/some/file.txt
               mysql://[user[:pass]@][host[:port]]/dbname?table-name
               postgresql://[user[:pass]@][host[:port]]/dbname?table-name

        [ COMPRESSED WITH zip | bzip2 | gzip ]

    WITH workers = 2,
         batch size = 25000,
         batch split = 5,
         reject file = '/tmp/pgloader/<table-name>.dat'
         log file = '/tmp/pgloader/pgloader.log',
         log level = debug | info | notice | warning | error | critical,
         truncate,
         fields [ optionally ] enclosed by '"',
         fields escaped by '\\',
         fields terminated by '\t',
         lines terminated by '\r\n',
         encoding = 'latin9',
         drop table,
         create table,
         create indexes,
         reset sequences

     SET guc-1 = 'value', guc-2 = 'value'

     PREPARE CLIENT WITH ( <lisp> )
     PREPARE SERVER WITH ( <sql> )

     INTO table-name [ WITH <options> SET <gucs> ]
          (
               field-name data-type field-desc [ with column options ],
               ...
          )
    USING (expression field-name other-field-name) as column-name,
          ...

    INTO table-name  [ WITH <options> SET <gucs> ]
         (
           *
         )

    TODO WHEN

    FINALLY ON CLIENT DO ( <lisp> )
            ON SERVER DO ( <lisp> )

    < data here if loading from stdin >
|#

;;
;; Some useful rules
;;
(defrule whitespace (+ (or #\space #\tab #\newline #\linefeed))
  (:constant 'whitespace))

(defrule ignore-whitespace (* whitespace)
  (:constant nil))

(defrule punct (or #\, #\- #\_)
  (:text t))

(defrule namestring (and (alpha-char-p character)
			 (* (or (alpha-char-p character)
				(digit-char-p character)
				punct)))
  (:text t))

(defrule quoted-namestring (and #\' namestring #\')
  (:destructure (open name close) (declare (ignore open close)) name))

(defrule name (or namestring quoted-namestring)
  (:text t))

(defrule trimmed-name (and ignore-whitespace name)
  (:destructure (whitespace name) (declare (ignore whitespace)) name))

;;
;; Parse PostgreSQL database connection strings
;;
;;  at postgresql://[user[:password]@][netloc][:port][/dbname]?table-name
;;
;; http://www.postgresql.org/docs/9.2/static/libpq-connect.html#LIBPQ-CONNSTRING
;;
(defrule dsn-port (and ":" (* (digit-char-p character)))
  (:destructure (colon digits &aux (port (coerce digits 'string)))
		(declare (ignore colon))
		(list :port (if (null digits) digits
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
		(append (list :host hostname) port)))

(defrule dsn-dbname (and "/" namestring)
  (:destructure (slash dbname)
		(declare (ignore slash))
		(list :dbname dbname)))

(defrule dsn-table-name (and "?" namestring)
  (:destructure (qm table-name)
    (declare (ignore qm))
    (list :table-name(coerce table-name 'string))))

(defrule dsn-prefix (and (+ (alpha-char-p character)) "://")
  (:destructure (p c-s-s &aux (prefix (coerce p 'string)))
    (declare (ignore c-s-s))
    (cond ((string= "postgresql" prefix) (list :type :postgresql))
	  ((string= "mysql" prefix)      (list :type :mysql))
	  (t (list :type :unknown)))))

(defrule db-connection-uri (and dsn-prefix
				(? dsn-user-password)
				(? dsn-hostname)
				dsn-dbname
				dsn-table-name)
  (:lambda (uri)
    (destructuring-bind (&key type
			      user
			      password
			      (host "localhost")
			      port
			      dbname
			      table-name)
	(apply #'append uri)
      (list :type type
	    :user user
	    :password password
	    :host host
	    :port (or port *default-postgresql-port*)
	    :dbname dbname
	    :table-name table-name))))

;;
;; The main target parsing
;;
;;  COPY postgresql://user@localhost:5432/dbname?foo
;;
(defrule target (and (~ "INTO") ignore-whitespace db-connection-uri)
  (:destructure (into whitespace target)
    (declare (ignore into whitespace))
    (destructuring-bind (&key type &allow-other-keys) target
      (unless (eq type :postgresql)
	(error "The target must be a PostgreSQL connection string."))
      target)))

;;
;; Source parsing
;;
;; Source is either a local filename, stdin, a MySQL connection with a
;; table-name, or an http uri.
;;

;; parsing filename
(defun filename-character-p (char)
  (or (member char #.(quote (coerce "/\\:.-_!@#$%^&*()" 'list)))
      (alphanumericp char)))

(defrule stdin (~ "stdin") (:constant (list :filename :stdin)))

(defrule filename (* (filename-character-p character))
  (:lambda (f)
    (list :filename (parse-namestring (coerce f 'string)))))

(defrule quoted-filename (and #\' filename #\')
  (:destructure (open f close) (declare (ignore open close)) f))

(defrule maybe-quoted-filename (or quoted-filename filename)
  (:identity t))

(defrule http-uri (and "http://" (* (filename-character-p character)))
  (:destructure (prefix url)
    (list :http (concatenate 'string prefix url))))

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

;;
;; Putting it all together, the COPY command
;;
;; The output format is Lisp code using the pgloader API.
;;
(defrule load (and ignore-whitespace source ignore-whitespace target)
  (:destructure (ws1 source ws2 target)
    (declare (ignore ws1 ws2))
    (destructuring-bind (&key table-name user password host port dbname
			      &allow-other-keys)
	target
      `(lambda (&key
		  (*pgconn-host* ,host)
		  (*pgconn-port* ,port)
		  (*pgconn-user* ,user)
		  (*pgconn-pass* ,password))
	 (pgloader.pgsql:copy-from-file ,dbname ,table-name ,source)))))

(defun parse-load (string)
  (parse 'load string))

(defun test-parsing ()
  (parse-load "
LOAD FROM http:///tapoueh.org/db.t
     INTO postgresql://localhost:6432/db?t"))

;;;
;;; The main target parsing
;;;
;;;  COPY postgresql://user@localhost:5432/dbname?foo
;;;

(in-package :pgloader.parser)

;;
;; Parse PostgreSQL database connection strings
;;
;;  at postgresql://[user[:password]@][netloc][:port][/dbname]?table-name
;;
;; http://www.postgresql.org/docs/9.2/static/libpq-connect.html#LIBPQ-CONNSTRING
;;
;; Also parse MySQL connection strings and syslog service definition
;; strings, using the same model.
;;
(defrule dsn-port (and ":" (* (digit-char-p character)))
  (:lambda (port)
    (bind (((_ digits &aux (port (coerce digits 'string))) port))
      (list :port (if (null digits) digits
                      (parse-integer port))))))

(defrule doubled-at-sign (and "@@") (:constant "@"))
(defrule doubled-colon   (and "::") (:constant ":"))
(defrule password (+ (or (not "@") doubled-at-sign)) (:text t))
(defrule username (and namestring (? (or doubled-at-sign doubled-colon)))
  (:text t))

(defrule dsn-user-password (and username
				(? (and ":" (? password)))
				"@")
  (:lambda (args)
    (bind (((username &optional password) (butlast args)))
      ;; password looks like '(":" "password")
      (list :user username :password (cadr password)))))

(defun hexdigit-char-p (character)
  (member character #. (quote (coerce "0123456789abcdefABCDEF" 'list))))

(defrule ipv4-part (and (digit-char-p character)
			(? (digit-char-p character))
			(? (digit-char-p character))))

(defrule ipv4 (and ipv4-part "." ipv4-part "." ipv4-part "." ipv4-part)
  (:lambda (ipv4)
    (list :ipv4 (text ipv4))))

;;; socket directory is unix only, so we can forbid ":" on the parsing
(defun socket-directory-character-p (char)
  (or (member char #.(quote (coerce "/.-_" 'list)))
      (alphanumericp char)))

(defrule socket-directory (and "unix:" (* (socket-directory-character-p character)))
  (:destructure (unix socket-directory)
		(declare (ignore unix))
    (list :unix (when socket-directory (text socket-directory)))))

(defrule network-name (and namestring (* (and "." namestring)))
  (:lambda (name)
    (let ((host (text name)))
      (list :host (unless (string= "" host) host)))))

(defrule hostname (or ipv4 socket-directory network-name)
  (:identity t))

(defrule dsn-hostname (and (? hostname) (? dsn-port))
  (:lambda (host-port)
    (destructuring-bind (host &optional port) host-port
      (append (list :host
                    (when host
                      (destructuring-bind (type &optional name) host
                        (ecase type
                          (:unix  (if name (cons :unix name) :unix))
                          (:ipv4  name)
                          (:host  name)))))
              port))))

(defrule dsn-dbname (and "/" (? namestring))
  (:destructure (slash dbname)
		(declare (ignore slash))
		(list :dbname dbname)))

(defrule qualified-table-name (and namestring "." namestring)
  (:destructure (schema dot table)
    (declare (ignore dot))
    (format nil "~a.~a" (text schema) (text table))))

(defrule dsn-table-name (and "?" (or qualified-table-name namestring))
  (:destructure (qm name)
    (declare (ignore qm))
    (list :table-name name)))

(defrule pgsql-prefix (and (or "postgresql" "postgres" "pgsql") "://")
  (:constant (list :type :postgresql)))

(defrule pgsql-uri (and pgsql-prefix
                        (? dsn-user-password)
                        (? dsn-hostname)
                        dsn-dbname
                        (? dsn-table-name))
  (:lambda (uri)
    (destructuring-bind (&key type
                              user
			      password
			      host
			      port
			      dbname
                              table-name)
        (apply #'append uri)
      ;; Default to environment variables as described in
      ;;  http://www.postgresql.org/docs/9.3/static/app-psql.html
      (list :type       type
            :user       (or user
                            (getenv-default "PGUSER"
                                            #+unix (getenv-default "USER")
                                            #-unix (getenv-default "UserName")))
            :password   (or password (getenv-default "PGPASSWORD"))
            :host       (or host     (getenv-default "PGHOST"
                                                     #+unix :unix
                                                     #-unix "localhost"))
            :port       (or port     (parse-integer
                                      (getenv-default "PGPORT" "5432")))
            :dbname     (or dbname   (getenv-default "PGDATABASE" user))
            :table-name table-name))))

(defrule get-pgsql-uri-from-environment-variable (and kw-getenv name)
  (:lambda (p-e-v)
    (bind (((_ varname) p-e-v))
      (let ((connstring (getenv-default varname)))
        (unless connstring
          (error "Environment variable ~s is unset." varname))
        (parse 'pgsql-uri connstring)))))

(defrule target (and kw-into (or pgsql-uri
                                 get-pgsql-uri-from-environment-variable))
  (:destructure (into target)
    (declare (ignore into))
    target))


(defun pgsql-connection-bindings (pg-db-uri gucs)
  "Generate the code needed to set PostgreSQL connection bindings."
  (destructuring-bind (&key ((:host pghost))
                            ((:port pgport))
                            ((:user pguser))
                            ((:password pgpass))
                            ((:dbname pgdb))
                            &allow-other-keys)
      pg-db-uri
    `((*pgconn-host* ',pghost)
      (*pgconn-port* ,pgport)
      (*pgconn-user* ,pguser)
      (*pgconn-pass* ,pgpass)
      (*pg-dbname*   ,pgdb)
      (*pg-settings* ',gucs)
      (pgloader.pgsql::*pgsql-reserved-keywords*
       (pgloader.pgsql:list-reserved-keywords ,pgdb)))))


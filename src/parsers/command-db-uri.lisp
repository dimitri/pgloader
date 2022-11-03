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
(defrule username (and (or #\_ (alpha-char-p character) (digit-char-p character))
                       (* (or (alpha-char-p character)
                              (digit-char-p character)
                              #\.
                              #\\
                              punct
                              doubled-at-sign
                              doubled-colon
                              )))
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

(defrule ipv6 (and #\[ (+ (or (digit-char-p character) ":")) #\])
  (:lambda (ipv6)
    (list :ipv6 (text ipv6))))

;;; socket directory is unix only, so we can forbid ":" on the parsing
(defun socket-directory-character-p (char)
  (or (member char #.(quote (coerce "/.-_" 'list)))
      (alphanumericp char)))

(defrule socket-directory (and "unix:"
                               (* (or (not ":") doubled-colon)))
  (:destructure (unix socket-directory)
		(declare (ignore unix))
    (list :unix (when socket-directory (text socket-directory)))))

;;;
;;; See https://en.wikipedia.org/wiki/Hostname#Restrictions_on_valid_hostnames
;;;
;;; The characters allowed in labels are a subset of the ASCII character
;;; set, consisting of characters a through z, A through Z, digits 0 through
;;; 9, and hyphen.
;;;
;;; This rule is known as the LDH rule (letters, digits, hyphen).
;;;
;;;  - Domain names are interpreted in case-independent manner.
;;;  - Labels may not start or end with a hyphen.
;;;  - An additional rule requires that top-level domain names should not be
;;;    all-numeric.
;;;
(defrule network-label-letters-digit (or (alpha-char-p character)
                                         (digit-char-p character)))

(defrule network-label-with-hyphen
    (and network-label-letters-digit
         (+ (or (and #\- network-label-letters-digit)
                network-label-letters-digit)))
  (:text t))

(defrule network-label-no-hyphen (+ network-label-letters-digit)
  (:text t))

(defrule network-label (or network-label-with-hyphen network-label-no-hyphen)
  (:identity t))

(defrule network-hostname (and network-label (* (and "." network-label)))
  (:lambda (name)
    (let ((host (text name)))
      (list :host (unless (string= "" host) host)))))

(defrule hostname (or ipv4 ipv6 socket-directory network-hostname)
  (:identity t))

(defun process-hostname (hostname)
  (destructuring-bind (type &optional name) hostname
    (ecase type
      (:unix  (if name (cons :unix name) :unix))
      (:ipv4  name)
      (:ipv6  name)
      (:host  name))))

(defrule dsn-hostname (and (? hostname) (? dsn-port))
  (:lambda (host-port)
    (destructuring-bind (host &optional port) host-port
      (append (list :host (when host (process-hostname host)))
              port))))

(defrule dsn-dbname (and "/" (? (or single-quoted-string
                                    (* (or (alpha-char-p character)
                                           (digit-char-p character)
                                           #\.
                                           punct)))))
  (:lambda (dbn)
    (list :dbname (text (second dbn)))))

(defrule dsn-option-ssl-disable "disable" (:constant :no))
(defrule dsn-option-ssl-allow   "allow"   (:constant :try))
(defrule dsn-option-ssl-prefer  "prefer"  (:constant :try))
(defrule dsn-option-ssl-require "require" (:constant :yes))

(defrule dsn-option-ssl (and "sslmode" "=" (or dsn-option-ssl-disable
                                               dsn-option-ssl-allow
                                               dsn-option-ssl-prefer
                                               dsn-option-ssl-require))
  (:lambda (ssl)
    (destructuring-bind (key e val) ssl
      (declare (ignore key e))
      (cons :use-ssl val))))

(defun get-pgsslmode (&optional (env-var-name "PGSSLMODE") default)
  "Get PGSSLMODE from the environment."
  (let ((pgsslmode (getenv-default env-var-name default)))
    (when pgsslmode
      (cdr (parse 'dsn-option-ssl (format nil "sslmode=~a" pgsslmode))))))

(defrule qualified-table-name (and maybe-quoted-namestring
                                   "."
                                   maybe-quoted-namestring)
  (:destructure (schema dot table)
    (declare (ignore dot))
    (cons (text schema) (text table))))

(defrule dsn-table-name (or qualified-table-name maybe-quoted-namestring)
  (:lambda (name)
    ;; we can't make a table instance yet here, because for that we need to
    ;; apply-identifier-case on it, and that requires to have initialized
    ;; the *pgsql-reserved-keywords*, and we can't do that before parsing
    ;; the target database connection string, can we?
    (cons :table-name name)))

(defrule dsn-option-table-name (and (? (and "tablename" "="))
                                    dsn-table-name)
  (:lambda (opt-tn)
    (bind (((_ table-name) opt-tn))
      table-name)))

(defrule uri-param (+ (not "&")) (:text t))

(defmacro make-dsn-option-rule (name param &optional (rule 'uri-param) fn)
  `(defrule ,name (and ,param "=" ,rule)
     (:lambda (x)
       (let ((cons (first (quri:url-decode-params (text x)))))
         (setf (car cons) (intern (string-upcase (car cons)) "KEYWORD"))
         (when ,fn
           (setf (cdr cons) (funcall ,fn (cdr cons))))
         cons))))

(make-dsn-option-rule dsn-option-host   "host" uri-param
                      (lambda (hostname)
                        (process-hostname
                         (parse 'hostname
                                ;; special case Unix Domain Socket paths
                                (cond ((char= (aref hostname 0) #\/)
                                       (format nil "unix:~a" hostname))
                                      (t hostname))))))
(make-dsn-option-rule dsn-option-port   "port"
                      (+ (digit-char-p character))
                      #'parse-integer)
(make-dsn-option-rule dsn-option-dbname "dbname")
(make-dsn-option-rule dsn-option-user   "user")
(make-dsn-option-rule dsn-option-pass   "password")

(defrule dsn-option (or dsn-option-ssl
                        dsn-option-host
                        dsn-option-port
                        dsn-option-dbname
                        dsn-option-user
                        dsn-option-pass
                        dsn-option-table-name))

(defrule another-dsn-option (and "&" dsn-option)
  (:lambda (source)
    (bind (((_ option) source)) option)))

(defrule dsn-options (and "?" dsn-option (* another-dsn-option))
  (:lambda (options)
    (destructuring-bind (qm opt1 opts) options
      (declare (ignore qm))
      (alexandria:alist-plist `(,opt1 ,@opts)))))

(defrule pgsql-prefix (and (or "postgresql" "postgres" "pgsql") "://")
  (:constant (list :type :postgresql)))

(defrule pgsql-uri (and pgsql-prefix
                        (? dsn-user-password)
                        (? dsn-hostname)
                        dsn-dbname
                        (? dsn-options))
  (:lambda (uri)
    (destructuring-bind (&key type
                              user
			      password
			      host
			      port
			      dbname
                              table-name
                              use-ssl)
        ;; we want the options to take precedence over the URI components,
        ;; so we destructure the URI again and prepend options here.
        (destructuring-bind (prefix user-pass host-port dbname options) uri
          (apply #'append options prefix user-pass host-port (list dbname)))
      ;; Default to environment variables as described in
      ;;  http://www.postgresql.org/docs/9.3/static/app-psql.html
      (declare (ignore type))
      (let ((pgconn
             (make-instance 'pgsql-connection
                            :user (or user
                                      (getenv-default "PGUSER"
                                                      #+unix
                                                      (getenv-default "USER")
                                                      #-unix
                                                      (getenv-default "UserName")))
                            :host (or host     (getenv-default "PGHOST"
                                                               #+unix :unix
                                                               #-unix "localhost"))
                            :port (or port     (parse-integer
                                                (getenv-default "PGPORT" "5432")))
                            :name (or dbname   (getenv-default "PGDATABASE" user))

                            :use-ssl (or use-ssl (get-pgsslmode "PGSSLMODE"))
                            :table-name table-name)))
        ;; Now set the password, maybe from ~/.pgpass
        (setf (db-pass pgconn)
              (or password
                  (getenv-default "PGPASSWORD")
                  (match-pgpass-file (db-host pgconn)
                                     (princ-to-string (db-port pgconn))
                                     (db-name pgconn)
                                     (db-user pgconn))))
        ;; And return our pgconn instance
        pgconn))))

(defrule target (and kw-into pgsql-uri)
  (:destructure (into target)
    (declare (ignore into))
    target))


(defun pgsql-connection-bindings (pg-db-uri gucs)
  "Generate the code needed to set PostgreSQL connection bindings."
  `((*pg-settings* (pgloader.pgsql:sanitize-user-gucs ',gucs))
    (*pgsql-reserved-keywords*
     (pgloader.pgsql:list-reserved-keywords ,pg-db-uri))))


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
(defrule single-line-comment (and "--" (+ (not #\Newline)) #\Newline)
  (:constant :comment))

(defrule multi-line-comment (and "/*" (+ (not "*/")) "*/")
  (:constant :comment))

(defrule comments (or single-line-comment multi-line-comment))

(defrule keep-a-single-whitespace (+ (or #\space #\tab #\newline #\linefeed))
  (:constant " "))

(defrule whitespace (+ (or #\space #\tab #\newline #\linefeed comments))
  (:constant 'whitespace))

(defrule ignore-whitespace (* whitespace)
  (:constant nil))

(defrule punct (or #\, #\- #\_)
  (:text t))

(defrule namestring (and (or #\_ (alpha-char-p character))
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


;;;
;;; Keywords
;;;
(defmacro def-keyword-rule (keyword)
  (let ((rule-name (read-from-string (format nil "kw-~a" keyword)))
	(constant  (read-from-string (format nil ":~a" keyword))))
    `(defrule ,rule-name (and ignore-whitespace (~ ,keyword) ignore-whitespace)
       (:constant ',constant))))

(eval-when (:load-toplevel :compile-toplevel :execute)
  (def-keyword-rule "load")
  (def-keyword-rule "data")
  (def-keyword-rule "from")
  (def-keyword-rule "csv")
  (def-keyword-rule "dbf")
  (def-keyword-rule "ixf")
  (def-keyword-rule "fixed")
  (def-keyword-rule "into")
  (def-keyword-rule "with")
  (def-keyword-rule "when")
  (def-keyword-rule "set")
  (def-keyword-rule "database")
  (def-keyword-rule "messages")
  (def-keyword-rule "matches")
  (def-keyword-rule "in")
  (def-keyword-rule "directory")
  (def-keyword-rule "registering")
  (def-keyword-rule "cast")
  (def-keyword-rule "column")
  (def-keyword-rule "target")
  (def-keyword-rule "columns")
  (def-keyword-rule "type")
  (def-keyword-rule "extra")
  (def-keyword-rule "include")
  (def-keyword-rule "drop")
  (def-keyword-rule "not")
  (def-keyword-rule "to")
  (def-keyword-rule "no")
  (def-keyword-rule "null")
  (def-keyword-rule "default")
  (def-keyword-rule "typemod")
  (def-keyword-rule "using")
  ;; option for loading from a file
  (def-keyword-rule "workers")
  (def-keyword-rule "batch")
  (def-keyword-rule "rows")
  (def-keyword-rule "size")
  (def-keyword-rule "concurrency")
  (def-keyword-rule "reject")
  (def-keyword-rule "file")
  (def-keyword-rule "log")
  (def-keyword-rule "level")
  (def-keyword-rule "encoding")
  (def-keyword-rule "decoding")
  (def-keyword-rule "truncate")
  (def-keyword-rule "lines")
  (def-keyword-rule "having")
  (def-keyword-rule "fields")
  (def-keyword-rule "optionally")
  (def-keyword-rule "enclosed")
  (def-keyword-rule "by")
  (def-keyword-rule "escaped")
  (def-keyword-rule "terminated")
  (def-keyword-rule "nullif")
  (def-keyword-rule "blank")
  (def-keyword-rule "skip")
  (def-keyword-rule "header")
  (def-keyword-rule "null")
  (def-keyword-rule "if")
  (def-keyword-rule "as")
  (def-keyword-rule "blanks")
  (def-keyword-rule "date")
  (def-keyword-rule "format")
  (def-keyword-rule "keep")
  (def-keyword-rule "trim")
  (def-keyword-rule "unquoted")
  ;; option for MySQL imports
  (def-keyword-rule "schema")
  (def-keyword-rule "only")
  (def-keyword-rule "drop")
  (def-keyword-rule "create")
  (def-keyword-rule "materialize")
  (def-keyword-rule "reset")
  (def-keyword-rule "table")
  (def-keyword-rule "name")
  (def-keyword-rule "names")
  (def-keyword-rule "tables")
  (def-keyword-rule "views")
  (def-keyword-rule "indexes")
  (def-keyword-rule "sequences")
  (def-keyword-rule "foreign")
  (def-keyword-rule "keys")
  (def-keyword-rule "downcase")
  (def-keyword-rule "quote")
  (def-keyword-rule "identifiers")
  (def-keyword-rule "including")
  (def-keyword-rule "excluding")
  ;; option for loading from an archive
  (def-keyword-rule "archive")
  (def-keyword-rule "before")
  (def-keyword-rule "after")
  (def-keyword-rule "finally")
  (def-keyword-rule "and")
  (def-keyword-rule "do")
  (def-keyword-rule "execute")
  (def-keyword-rule "filename")
  (def-keyword-rule "filenames")
  (def-keyword-rule "matching")
  (def-keyword-rule "first")
  (def-keyword-rule "all"))

(defrule kw-auto-increment (and "auto_increment" (* (or #\Tab #\Space)))
  (:constant :auto-increment))


;;;
;;; Regular Expression support, quoted as-you-like
;;;
(defun process-quoted-regex (pr)
  "Helper function to process different kinds of quotes for regexps"
  (destructuring-bind (open regex close) pr
      (declare (ignore open close))
      `(:regex ,(text regex))))

(defrule single-quoted-regex (and #\' (+ (not #\')) #\')
  (:function process-quoted-regex))

(defrule double-quoted-regex (and #\" (+ (not #\")) #\")
  (:function process-quoted-regex))

(defrule parens-quoted-regex (and #\( (+ (not #\))) #\))
  (:function process-quoted-regex))

(defrule braces-quoted-regex (and #\{ (+ (not #\})) #\})
  (:function process-quoted-regex))

(defrule chevron-quoted-regex (and #\< (+ (not #\>)) #\>)
  (:function process-quoted-regex))

(defrule brackets-quoted-regex (and #\[ (+ (not #\])) #\])
  (:function process-quoted-regex))

(defrule slash-quoted-regex (and #\/ (+ (not #\/)) #\/)
  (:function process-quoted-regex))

(defrule pipe-quoted-regex (and #\| (+ (not #\|)) #\|)
  (:function process-quoted-regex))

(defrule sharp-quoted-regex (and #\# (+ (not #\#)) #\#)
  (:function process-quoted-regex))

(defrule quoted-regex (and "~" (or single-quoted-regex
				   double-quoted-regex
				   parens-quoted-regex
				   braces-quoted-regex
				   chevron-quoted-regex
				   brackets-quoted-regex
				   slash-quoted-regex
				   pipe-quoted-regex
				   sharp-quoted-regex))
  (:lambda (qr)
    (destructuring-bind (tilde regex) qr
      (declare (ignore tilde))
      regex)))


;;;
;;; The main target parsing
;;;
;;;  COPY postgresql://user@localhost:5432/dbname?foo
;;;
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
  (:destructure (colon digits &aux (port (coerce digits 'string)))
		(declare (ignore colon))
		(list :port (if (null digits) digits
				(parse-integer port)))))

(defrule doubled-at-sign (and "@@") (:constant "@"))
(defrule doubled-colon   (and "::") (:constant ":"))
(defrule password (+ (or (not "@") doubled-at-sign)) (:text t))
(defrule username (and namestring (? (or doubled-at-sign doubled-colon)))
  (:text t))

(defrule dsn-user-password (and username
				(? (and ":" (? password)))
				"@")
  (:lambda (args)
    (destructuring-bind (username &optional password)
	(butlast args)
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
    (list :host (text name))))

(defrule hostname (or ipv4 socket-directory network-name)
  (:identity t))

(defrule dsn-hostname (and (? hostname) (? dsn-port))
  (:destructure (hostname &optional port)
		(append (list :host hostname) port)))

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

(defrule dsn-prefix (and (+ (alpha-char-p character)) "://")
  (:destructure (p c-s-s &aux (prefix (coerce p 'string)))
    (declare (ignore c-s-s))
    (cond ((string= "postgresql" prefix) (list :type :postgresql))
	  ((string= "mysql" prefix)      (list :type :mysql))
	  ((string= "syslog" prefix)     (list :type :syslog))
	  (t (list :type :unknown)))))

(defrule db-connection-uri (and dsn-prefix
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
      ;;
      ;; Default to environment variables as described in
      ;;  http://www.postgresql.org/docs/9.3/static/app-psql.html
      ;;  http://dev.mysql.com/doc/refman/5.0/en/environment-variables.html
      ;;
      (let ((user
	     (or user
		 (case type
		   (:postgresql (getenv-default "PGUSER" (getenv-default "USER")))
		   (:mysql      (getenv-default "USER")))))
          (password (or password
              (case type
                (:postgresql (getenv-default "PGPASSWORD"))
                (:mysql (getenv-default "MYSQL_PWD"))))))
       (list :type type
	     :user user
	     :password password
	     :host (or (when host
			 (destructuring-bind (type &optional name) host
			   (ecase type
			     (:unix  (if name (cons :unix name) :unix))
			     (:ipv4  name)
			     (:host  name))))
		       (case type
			 (:postgresql (getenv-default "PGHOST"
						      #+unix :unix
						      #-unix "localhost"))
			 (:mysql      (getenv-default "MYSQL_HOST" "localhost"))))
	     :port (or port
		       (parse-integer
			;; avoid a NIL is not a STRING style warning by
			;; using ecase here
			(ecase type
			  (:postgresql (getenv-default "PGPORT" "5432"))
			  (:mysql      (getenv-default "MYSQL_TCP_PORT" "3306")))))
	     :dbname (or dbname
			 (case type
			   (:postgresql (getenv-default "PGDATABASE" user))))
	     :table-name table-name)))))

(defrule target (and kw-into db-connection-uri)
  (:destructure (into target)
    (declare (ignore into))
    (destructuring-bind (&key type &allow-other-keys) target
      (unless (eq type :postgresql)
	(error "The target must be a PostgreSQL connection string."))
      target)))

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

(defun mysql-connection-bindings (my-db-uri)
  "Generate the code needed to set MySQL connection bindings."
  (destructuring-bind (&key ((:host myhost))
                            ((:port myport))
                            ((:user myuser))
                            ((:password mypass))
                            ((:dbname mydb))
                            &allow-other-keys)
      my-db-uri
    `((*myconn-host* ',myhost)
      (*myconn-port* ,myport)
      (*myconn-user* ,myuser)
      (*myconn-pass* ,mypass)
      (*my-dbname*   ,mydb))))


;;;
;;; Source parsing
;;;
;;; Source is either a local filename, stdin, a MySQL connection with a
;;; table-name, or an http uri.
;;;

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
    (destructuring-bind (open f close) q-f
      (declare (ignore open close))
      (list :filename (parse-namestring (coerce f 'string))))))

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

(defrule database-source (and kw-load kw-database kw-from
			      db-connection-uri)
  (:lambda (source)
    (destructuring-bind (l d f uri) source
      (declare (ignore l d f))
      uri)))


;;;
;;; Parsing GUCs and WITH options for loading from MySQL and from file.
;;;
(defun optname-char-p (char)
  (and (or (alphanumericp char)
	   (char= char #\-)		; support GUCs
	   (char= char #\_))		; support GUCs
       (not (char= char #\Space))))

(defrule optname-element (* (optname-char-p character)))
(defrule another-optname-element (and keep-a-single-whitespace optname-element))

(defrule optname (and optname-element (* another-optname-element))
  (:lambda (source)
    (string-trim " " (text source))))

(defun optvalue-char-p (char)
  (not (member char '(#\, #\; #\=) :test #'char=)))

(defrule optvalue (+ (optvalue-char-p character))
  (:text t))

(defrule equal-sign (and (* whitespace) #\= (* whitespace))
  (:constant :equal))

(defrule option-workers (and kw-workers equal-sign (+ (digit-char-p character)))
  (:lambda (workers)
    (destructuring-bind (w e nb) workers
      (declare (ignore w e))
      (cons :workers (parse-integer (text nb))))))

(defrule option-batch-rows (and kw-batch kw-rows equal-sign
                                (+ (digit-char-p character)))
  (:lambda (batch-rows)
    (destructuring-bind (b r e nb) batch-rows
      (declare (ignore b r e))
      (cons :batch-rows (parse-integer (text nb))))))

(defrule byte-size-multiplier (or #\k #\M #\G #\T #\P)
  (:lambda (multiplier)
    (case (aref multiplier 0)
      (#\k 10)
      (#\M 20)
      (#\G 30)
      (#\T 40)
      (#\P 50))))

(defrule byte-size-unit (and ignore-whitespace (? byte-size-multiplier) #\B)
  (:lambda (unit)
    (destructuring-bind (ws &optional (multiplier 1) byte) unit
      (declare (ignore ws byte))
      (expt 2 multiplier))))

(defrule batch-size (and (+ (digit-char-p character)) byte-size-unit)
  (:lambda (batch-size)
    (destructuring-bind (nb unit) batch-size
      (* (parse-integer (text nb)) unit))))

(defrule option-batch-size (and kw-batch kw-size equal-sign batch-size)
  (:lambda (batch-size)
    (destructuring-bind (b s e val) batch-size
      (declare (ignore b s e))
      (cons :batch-size val))))

(defrule option-batch-concurrency (and kw-batch kw-concurrency equal-sign
                                       (+ (digit-char-p character)))
  (:lambda (batch-concurrency)
    (destructuring-bind (b c e nb) batch-concurrency
      (declare (ignore b c e))
      (cons :batch-concurrency (parse-integer (text nb))))))

(defun batch-control-bindings (options)
  "Generate the code needed to add batch-control"
  `((*copy-batch-rows*    (or ,(getf options :batch-rows) *copy-batch-rows*))
    (*copy-batch-size*    (or ,(getf options :batch-size) *copy-batch-size*))
    (*concurrent-batches* (or ,(getf options :batch-concurrency) *concurrent-batches*))))

(defun remove-batch-control-option (options
                                    &key
                                      (option-list '(:batch-rows
                                                     :batch-size
                                                     :batch-concurrency))
                                      extras)
  "Given a list of options, remove the generic ones that should already have
   been processed."
  (loop :for (k v) :on options :by #'cddr
     :unless (member k (append option-list extras))
     :append (list k v)))

(defmacro make-option-rule (name rule &optional option)
  "Generates a rule named NAME to parse RULE and return OPTION."
  (let* ((bindings
	  (loop for element in rule
	     unless (member element '(and or))
	     collect (if (and (typep element 'list)
			      (eq '? (car element))) 'no (gensym))))
	 (ignore (loop for b in bindings unless (eq 'no b) collect b))
	 (option-name (intern (string-upcase (format nil "option-~a" name))))
	 (option      (or option (intern (symbol-name name) :keyword))))
    `(defrule ,option-name ,rule
       (:destructure ,bindings
		     (declare (ignore ,@ignore))
		     (cons ,option (null no))))))

(make-option-rule include-drop    (and kw-include (? kw-no) kw-drop))
(make-option-rule truncate        (and (? kw-no) kw-truncate))
(make-option-rule create-tables   (and kw-create (? kw-no) kw-tables))
(make-option-rule create-indexes  (and kw-create (? kw-no) kw-indexes))
(make-option-rule reset-sequences (and kw-reset  (? kw-no) kw-sequences))
(make-option-rule foreign-keys    (and (? kw-no) kw-foreign kw-keys))

(defrule option-schema-only (and kw-schema kw-only)
  (:constant (cons :schema-only t)))

(defrule option-data-only (and kw-data kw-only)
  (:constant (cons :data-only t)))

(defrule option-identifiers-case (and (or kw-downcase kw-quote) kw-identifiers)
  (:lambda (id-case)
    (destructuring-bind (action id) id-case
      (declare (ignore id))
      (cons :identifier-case action))))

(defrule mysql-option (or option-workers
                          option-batch-rows
                          option-batch-size
                          option-batch-concurrency
			  option-truncate
			  option-data-only
			  option-schema-only
			  option-include-drop
			  option-create-tables
			  option-create-indexes
			  option-reset-sequences
			  option-foreign-keys
			  option-identifiers-case))

(defrule comma (and ignore-whitespace #\, ignore-whitespace)
  (:constant :comma))

(defrule another-mysql-option (and comma mysql-option)
  (:lambda (source)
    (destructuring-bind (comma option) source
      (declare (ignore comma))
      option)))

(defrule mysql-option-list (and mysql-option (* another-mysql-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist (list* opt1 opts)))))

(defrule mysql-options (and kw-with mysql-option-list)
  (:lambda (source)
    (destructuring-bind (w opts) source
      (declare (ignore w))
      (cons :mysql-options opts))))

;; we don't validate GUCs, that's PostgreSQL job.
(defrule generic-optname optname-element
  (:text t))

(defrule generic-value (and #\' (* (not #\')) #\')
  (:lambda (quoted)
    (destructuring-bind (open value close) quoted
      (declare (ignore open close))
      (text value))))

(defrule generic-option (and generic-optname
			     (or equal-sign kw-to)
			     generic-value)
  (:lambda (source)
    (destructuring-bind (name es value) source
      (declare (ignore es))
      (cons name value))))

(defrule another-generic-option (and comma generic-option)
  (:lambda (source)
    (destructuring-bind (comma option) source
      (declare (ignore comma))
      option)))

(defrule generic-option-list (and generic-option (* another-generic-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      ;; here we want an alist
      (list* opt1 opts))))

(defrule gucs (and kw-set generic-option-list)
  (:lambda (source)
    (destructuring-bind (set gucs) source
      (declare (ignore set))
      (cons :gucs gucs))))


#|
       BEFORE LOAD DO $$ sql $$

        LOAD CSV FROM '*/GeoLiteCity-Blocks.csv'      ...
        LOAD DBF FROM '*/GeoLiteCity-Location.csv'    ...

       FINALLY DO     $$ sql $$;
|#
(defrule double-dollar (and ignore-whitespace #\$ #\$ ignore-whitespace)
  (:constant :double-dollars))

(defrule dollar-quoted (and double-dollar (* (not double-dollar)) double-dollar)
  (:lambda (dq)
    (destructuring-bind (open quoted close) dq
      (declare (ignore open close))
      (text quoted))))

(defrule another-dollar-quoted (and comma dollar-quoted)
  (:lambda (source)
    (destructuring-bind (comma quoted) source
      (declare (ignore comma))
      quoted)))

(defrule dollar-quoted-list (and dollar-quoted (* another-dollar-quoted))
  (:lambda (source)
    (destructuring-bind (dq1 dqs) source
      (list* dq1 dqs))))

(defrule before-load-do (and kw-before kw-load kw-do dollar-quoted-list)
  (:lambda (bld)
    (destructuring-bind (before load do quoted) bld
      (declare (ignore before load do))
      quoted)))

(defrule sql-file (or maybe-quoted-filename)
  (:lambda (filename)
    (destructuring-bind (kind path) filename
      (ecase kind
        (:filename
         (pgloader.sql:read-queries (merge-pathnames path *cwd*)))))))

(defrule before-load-execute (and kw-before kw-load kw-execute sql-file)
  (:lambda (ble)
    (destructuring-bind (before load execute sql) ble
      (declare (ignore before load execute))
      sql)))

(defrule before-load (or before-load-do before-load-execute)
  (:lambda (before)
    (cons :before before)))

(defrule finally-do (and kw-finally kw-do dollar-quoted-list)
  (:lambda (fd)
    (destructuring-bind (finally do quoted) fd
      (declare (ignore finally do))
      quoted)))

(defrule finally-execute (and kw-finally kw-execute sql)
  (:lambda (fe)
    (destructuring-bind (finally execute sql) fe
      (declare (ignore finally execute))
      sql)))

(defrule finally (or finally-do finally-execute)
  (:lambda (finally)
    (cons :finally finally)))

(defrule after-load-do (and kw-after kw-load kw-do dollar-quoted-list)
  (:lambda (fd)
    (destructuring-bind (after load do quoted) fd
      (declare (ignore after load do))
      quoted)))

(defrule after-load-execute (and kw-after kw-load kw-execute sql-file)
  (:lambda (fd)
    (destructuring-bind (after load execute sql) fd
      (declare (ignore after load execute))
      sql)))

(defrule after-load (or after-load-do after-load-execute)
  (:lambda (after)
    (cons :after after)))

(defun sql-code-block (dbname state commands label)
  "Return lisp code to run COMMANDS against DBNAME, updating STATE."
  (when commands
    `(with-stats-collection (,label
                             :dbname ,dbname
                             :state ,state
                             :use-result-as-read t
                             :use-result-as-rows t)
       (with-pgsql-transaction (:dbname ,dbname)
	 (loop for command in ',commands
	    do
	      (log-message :notice command)
	      (pgsql-execute command :client-min-messages :error)
            counting command)))))


;;;
;;; Now parsing CAST rules for migrating from MySQL
;;;
(defrule cast-typemod-guard (and kw-when sexp)
  (:destructure (w expr) (declare (ignore w)) (cons :typemod expr)))

(defrule cast-default-guard (and kw-when kw-default quoted-string)
  (:destructure (w d value) (declare (ignore w d)) (cons :default value)))

(defrule cast-source-guards (* (or cast-default-guard
				   cast-typemod-guard))
  (:lambda (guards)
    (alexandria:alist-plist guards)))

;; at the moment we only know about extra auto_increment
(defrule cast-source-extra (and kw-with kw-extra kw-auto-increment)
  (:constant (list :auto-increment t)))

(defrule cast-source-type (and kw-type trimmed-name)
  (:destructure (kw name) (declare (ignore kw)) (list :type name)))

(defrule table-column-name (and namestring "." namestring)
  (:destructure (table-name dot column-name)
    (declare (ignore dot))
    (list :column (cons (text table-name) (text column-name)))))

(defrule cast-source-column (and kw-column table-column-name)
  ;; well, we want namestring . namestring
  (:destructure (kw name) (declare (ignore kw)) name))

(defrule cast-source (and (or cast-source-type cast-source-column)
			  (? cast-source-extra)
			  (? cast-source-guards)
			  ignore-whitespace)
  (:lambda (source)
    (destructuring-bind (name-and-type opts guards ws) source
      (declare (ignore ws))
      (destructuring-bind (&key (default nil d-s-p)
				(typemod nil t-s-p)
				&allow-other-keys)
	  guards
	(destructuring-bind (&key (auto-increment nil ai-s-p)
				  &allow-other-keys)
	    opts
	  `(,@name-and-type
		,@(when t-s-p (list :typemod typemod))
		,@(when d-s-p (list :default default))
		,@(when ai-s-p (list :auto-increment auto-increment))))))))

(defrule cast-type-name (and (alpha-char-p character)
			     (* (or (alpha-char-p character)
				    (digit-char-p character))))
  (:text t))

(defrule cast-to-type (and kw-to cast-type-name ignore-whitespace)
  (:lambda (source)
    (destructuring-bind (to type-name ws) source
      (declare (ignore to ws))
      (list :type type-name))))

(defrule cast-keep-default  (and kw-keep kw-default)
  (:constant (list :drop-default nil)))

(defrule cast-keep-typemod (and kw-keep kw-typemod)
  (:constant (list :drop-typemod nil)))

(defrule cast-keep-not-null (and kw-keep kw-not kw-null)
  (:constant (list :drop-not-null nil)))

(defrule cast-drop-default  (and kw-drop kw-default)
  (:constant (list :drop-default t)))

(defrule cast-drop-typemod (and kw-drop kw-typemod)
  (:constant (list :drop-typemod t)))

(defrule cast-drop-not-null (and kw-drop kw-not kw-null)
  (:constant (list :drop-not-null t)))

(defrule cast-def (+ (or cast-to-type
			 cast-keep-default
			 cast-drop-default
			 cast-keep-typemod
			 cast-drop-typemod
			 cast-keep-not-null
			 cast-drop-not-null))
  (:lambda (source)
    (destructuring-bind
	  (&key type drop-default drop-typemod drop-not-null &allow-other-keys)
	(apply #'append source)
      (list :type type
	    :drop-default drop-default
	    :drop-typemod drop-typemod
	    :drop-not-null drop-not-null))))

(defun function-name-character-p (char)
  (or (member char #.(quote (coerce "/:.-%" 'list)))
      (alphanumericp char)))

(defrule function-name (* (function-name-character-p character))
  (:text t))

(defrule cast-function (and kw-using function-name)
  (:lambda (function)
    (destructuring-bind (using fname) function
      (declare (ignore using))
      (intern (string-upcase fname) :pgloader.transforms))))

(defun fix-target-type (source target)
  "When target has :type nil, steal the source :type definition."
  (if (getf target :type)
      target
      (loop
	 for (key value) on target by #'cddr
	 append (list key (if (eq :type key) (getf source :type) value)))))

(defrule cast-rule (and cast-source (? cast-def) (? cast-function))
  (:lambda (cast)
    (destructuring-bind (source target function) cast
      (list :source source
	    :target (fix-target-type source target)
	    :using function))))

(defrule another-cast-rule (and comma cast-rule)
  (:lambda (source)
    (destructuring-bind (comma rule) source
      (declare (ignore comma))
      rule)))

(defrule cast-rule-list (and cast-rule (* another-cast-rule))
  (:lambda (source)
    (destructuring-bind (rule1 rules) source
      (list* rule1 rules))))

(defrule casts (and kw-cast cast-rule-list)
  (:lambda (source)
    (destructuring-bind (c casts) source
      (declare (ignore c))
      (cons :casts casts))))


;;;
;;; Materialize views by copying their data over, allows for doing advanced
;;; ETL processing by having parts of the processing happen on the MySQL
;;; query side.
;;;
(defrule view-name (and (alpha-char-p character)
			(* (or (alpha-char-p character)
			       (digit-char-p character)
			       #\_)))
  (:text t))

(defrule view-sql (and kw-as dollar-quoted)
  (:destructure (as sql) (declare (ignore as)) sql))

(defrule view-definition (and view-name (? view-sql))
  (:destructure (name sql) (cons name sql)))

(defrule another-view-definition (and comma view-definition)
  (:lambda (source)
    (destructuring-bind (comma view) source
      (declare (ignore comma))
      view)))

(defrule views-list (and view-definition (* another-view-definition))
  (:lambda (vlist)
    (destructuring-bind (view1 views) vlist
      (list* view1 views))))

(defrule materialize-all-views (and kw-materialize kw-all kw-views)
  (:constant :all))

(defrule materialize-view-list (and kw-materialize kw-views views-list)
  (:destructure (mat views list) (declare (ignore mat views)) list))

(defrule materialize-views (or materialize-view-list materialize-all-views)
  (:lambda (views)
    (cons :views views)))


;;;
;;; Including only some tables or excluding some others
;;;
(defrule namestring-or-regex (or quoted-namestring quoted-regex))

(defrule another-namestring-or-regex (and comma namestring-or-regex)
  (:lambda (source)
    (destructuring-bind (comma re) source
      (declare (ignore comma))
      re)))

(defrule filter-list (and namestring-or-regex (* another-namestring-or-regex))
  (:lambda (source)
    (destructuring-bind (filter1 filters) source
      (list* filter1 filters))))

(defrule including (and kw-including kw-only kw-table kw-names kw-matching
			filter-list)
  (:lambda (source)
    (destructuring-bind (i o table n m filter-list) source
      (declare (ignore i o table n m))
      (cons :including filter-list))))

(defrule excluding (and kw-excluding kw-table kw-names kw-matching filter-list)
  (:lambda (source)
    (destructuring-bind (e table n m filter-list) source
      (declare (ignore e table n m))
      (cons :excluding filter-list))))


;;;
;;; Per table encoding options, because MySQL is so bad at encoding...
;;;
(defrule decoding-table-as (and kw-decoding kw-table kw-names kw-matching
                                filter-list
                                kw-as encoding)
  (:lambda (source)
    (destructuring-bind (d table n m filter-list as encoding) source
      (declare (ignore d table n m as))
      (cons encoding filter-list))))

(defrule decoding-tables-as (+ decoding-table-as)
  (:lambda (tables)
    (cons :decoding tables)))


;;;
;;; Allow clauses to appear in any order
;;;
(defrule load-mysql-optional-clauses (* (or mysql-options
                                            gucs
                                            casts
                                            materialize-views
                                            including
                                            excluding
                                            decoding-tables-as
                                            before-load
                                            after-load))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule load-mysql-command (and database-source target
                                 load-mysql-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source target clauses) command
      `(,source ,target ,@clauses))))


;;; LOAD DATABASE FROM mysql://
(defrule load-mysql-database load-mysql-command
  (:lambda (source)
    (destructuring-bind (my-db-uri pg-db-uri
                                   &key
				   gucs casts views before after
                                   ((:mysql-options options))
				   ((:including incl))
                                   ((:excluding excl))
                                   ((:decoding decoding-as)))
	source
      (destructuring-bind (&key ((:dbname mydb)) table-name
				&allow-other-keys)
	  my-db-uri
        (destructuring-bind (&key ((:dbname pgdb)) &allow-other-keys) pg-db-uri
          `(lambda ()
             (let* ((state-before  (pgloader.utils:make-pgstate))
                    (*state*       (or *state* (pgloader.utils:make-pgstate)))
                    (state-idx     (pgloader.utils:make-pgstate))
                    (state-after   (pgloader.utils:make-pgstate))
                    (pgloader.mysql:*cast-rules* ',casts)
                    ,@(mysql-connection-bindings my-db-uri)
                    ,@(pgsql-connection-bindings pg-db-uri gucs)
                    ,@(batch-control-bindings options)
		    (source
		     (make-instance 'pgloader.mysql::copy-mysql
				    :target-db ,pgdb
				    :source-db ,mydb)))

               ,(sql-code-block pgdb 'state-before before "before load")

               (pgloader.mysql:copy-database source
                                             ,@(when table-name
                                                     `(:only-tables ',(list table-name)))
                                             :including ',incl
                                             :excluding ',excl
                                             :decoding-as ',decoding-as
                                             :materialize-views ',views
                                             :state-before state-before
                                             :state-after state-after
                                             :state-indexes state-idx
                                             ,@(remove-batch-control-option options))

               ,(sql-code-block pgdb 'state-after after "after load")

               (report-full-summary "Total import time" *state*
                                    :before   state-before
                                    :finally  state-after
                                    :parallel state-idx))))))))


;;;
;;; LOAD DATABASE FROM SQLite
;;;
#|
load database
     from sqlite:///Users/dim/Downloads/lastfm_tags.db
     into postgresql:///tags

 with drop tables, create tables, create indexes, reset sequences

  set work_mem to '16MB', maintenance_work_mem to '512 MB';
|#
(defrule sqlite-option (or option-batch-rows
                           option-batch-size
                           option-batch-concurrency
                           option-truncate
			   option-data-only
			   option-schema-only
			   option-include-drop
			   option-create-tables
			   option-create-indexes
			   option-reset-sequences))

(defrule another-sqlite-option (and comma sqlite-option)
  (:lambda (source)
    (destructuring-bind (comma option) source
      (declare (ignore comma))
      option)))

(defrule sqlite-option-list (and sqlite-option (* another-sqlite-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist (list* opt1 opts)))))

(defrule sqlite-options (and kw-with sqlite-option-list)
  (:lambda (source)
    (destructuring-bind (w opts) source
      (declare (ignore w))
      (cons :sqlite-options opts))))

(defrule sqlite-db-uri (and "sqlite://" filename)
  (:lambda (source)
    (destructuring-bind (prefix filename) source
      (declare (ignore prefix))
      (destructuring-bind (type path) filename
	(declare (ignore type))
	(list :sqlite path)))))

(defrule sqlite-uri (or sqlite-db-uri http-uri maybe-quoted-filename))
(defrule sqlite-source (and kw-load kw-database kw-from sqlite-uri)
  (:destructure (l d f u)
		(declare (ignore l d f))
		u))

(defrule load-sqlite-optional-clauses (* (or sqlite-options
                                             gucs
                                             including
                                             excluding))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule load-sqlite-command (and sqlite-source target
                                  load-sqlite-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source target clauses) command
      `(,source ,target ,@clauses))))

(defrule load-sqlite-database load-sqlite-command
  (:lambda (source)
    (destructuring-bind (sqlite-uri pg-db-uri
                                    &key
                                    ((:sqlite-options options)) gucs incl excl)
        source
      (destructuring-bind (&key dbname table-name &allow-other-keys)
	  pg-db-uri
	`(lambda ()
	   (let* ((state-before   (pgloader.utils:make-pgstate))
		  (*state*        (pgloader.utils:make-pgstate))
                  ,@(pgsql-connection-bindings pg-db-uri gucs)
                  ,@(batch-control-bindings options)
		  (db
		   ,(destructuring-bind (kind url) sqlite-uri
		     (ecase kind
		       (:http     `(with-stats-collection
				       ("download" :state state-before)
				     (pgloader.archive:http-fetch-file ,url)))
		       (:sqlite url)
		       (:filename url))))
		  (db
		   (if (string= "zip" (pathname-type db))
		       (progn
			 (with-stats-collection ("extract" :state state-before)
			   (let ((d (pgloader.archive:expand-archive db)))
			     (merge-pathnames
			      (make-pathname :name (pathname-name db)
					     :type "db")
			      d))))
		       db))
		  (source
		   (make-instance 'pgloader.sqlite::copy-sqlite
				  :target-db ,dbname
				  :source-db db)))
	     (pgloader.sqlite:copy-database source
					    :state-before state-before
					   ,@(when table-name
					     `(:only-tables ',(list table-name)))
					   :including ',incl
					   :excluding ',excl
					   ,@(remove-batch-control-option options))))))))



;;;
;;; LOAD MESSAGES FROM syslog
;;;
#|
    LOAD MESSAGES FROM syslog://localhost:10514/

        WHEN MATCHES rsyslog-msg IN apache
         REGISTERING timestamp, ip, rest
        INTO postgresql://localhost/db?logs.apache
         SET guc_1 = 'value', guc_2 = 'other value'

        WHEN MATCHES rsyslog-msg IN others
         REGISTERING timestamp, app-name, data
        INTO postgresql://localhost/db?logs.others
         SET guc_1 = 'value', guc_2 = 'other value'

        WITH apache = rsyslog
             DATA   = IP REST
             IP     = 1*3DIGIT \".\" 1*3DIGIT \".\"1*3DIGIT \".\"1*3DIGIT
             REST   = ~/.*/

        WITH others = rsyslog;
|#
(defrule rule-name (and (alpha-char-p character)
			(+ (abnf::rule-name-character-p character)))
  (:lambda (name)
    (text name)))

(defrule rules (* (not (or kw-registering
			   kw-with
			   kw-when
			   kw-set
			   end-of-command)))
  (:text t))

(defrule rule-name-list (and rule-name
			     (+ (and "," ignore-whitespace rule-name)))
  (:lambda (list)
    (destructuring-bind (name names) list
      (list* name (mapcar (lambda (x)
			    (destructuring-bind (c w n) x
			      (declare (ignore c w))
			      n)) names)))))

(defrule syslog-grammar (and kw-with rule-name equal-sign rule-name rules)
  (:lambda (grammar)
    (destructuring-bind (w top-level e gram abnf) grammar
      (declare (ignore w e))
      (let* ((default-abnf-grammars
	      `(("rsyslog" . ,abnf:*abnf-rsyslog*)
		("syslog"  . ,abnf:*abnf-rfc5424-syslog-protocol*)
		("syslog-draft-15" . ,abnf:*abnf-rfc-syslog-draft-15*)))
	     (grammar (cdr (assoc gram default-abnf-grammars :test #'string=))))
	(cons top-level
	      (concatenate 'string
			   abnf
			   '(#\Newline #\Newline)
			   grammar))))))

(defrule register-groups (and kw-registering rule-name-list)
  (:lambda (groups)
    (destructuring-bind (reg rule-names) groups
      (declare (ignore reg))
      rule-names)))

(defrule syslog-match (and kw-when
			   kw-matches rule-name kw-in rule-name
			   register-groups
			   target
			   (? gucs))
  (:lambda (matches)
    (destructuring-bind (w m top-level i rule-name groups target gucs) matches
      (declare (ignore w m i))
      (list :target target
	    :gucs gucs
	    :top-level top-level
	    :grammar rule-name
	    :groups groups))))

(defrule syslog-connection-uri (and dsn-prefix dsn-hostname (? "/"))
  (:lambda (syslog)
    (destructuring-bind (prefix host-port slash) syslog
      (declare (ignore slash))
      (destructuring-bind (&key type host port)
	  (append prefix host-port)
	(list :type type
	      :host host
	      :port port)))))

(defrule syslog-source (and ignore-whitespace
			      kw-load kw-messages kw-from
			      syslog-connection-uri)
  (:lambda (source)
    (destructuring-bind (nil l d f uri) source
      (declare (ignore l d f))
      uri)))

(defrule load-syslog-messages (and syslog-source
				   (+ syslog-match)
				   (+ syslog-grammar))
  (:lambda (syslog)
    (destructuring-bind (syslog-server matches grammars)
	syslog
      (destructuring-bind (&key ((:host syslog-host))
				((:port syslog-port))
				&allow-other-keys)
	  syslog-server
	(let ((scanners
	       (loop
		  for match in matches
		  collect (destructuring-bind (&key target
						    gucs
						    top-level
						    grammar
						    groups)
			      match
			    (list :target target
				  :gucs gucs
				  :parser (abnf:parse-abnf-grammar
					    (cdr (assoc grammar grammars
							:test #'string=))
					    top-level
					    :registering-rules groups)
				  :groups groups)))))
	  `(lambda ()
	     (let ((scanners ',scanners))
	       (pgloader.syslog:stream-messages :host ,syslog-host
						:port ,syslog-port
						:scanners scanners))))))))


#|
    LOAD DBF FROM '/Users/dim/Downloads/comsimp2013.dbf'
        INTO postgresql://dim@localhost:54393/dim?comsimp2013
        WITH truncate, create table, table name = 'comsimp2013'
|#
(defrule option-create-table (and kw-create kw-table)
  (:constant (cons :create-table t)))

(defrule quoted-table-name (and #\' (or qualified-table-name namestring) #\')
  (:lambda (qtn)
    (destructuring-bind (open name close) qtn
      (declare (ignore open close))
      name)))

(defrule option-table-name (and kw-table kw-name equal-sign quoted-table-name)
  (:lambda (tn)
    (destructuring-bind (table name e table-name) tn
      (declare (ignore table name e))
      (cons :table-name (text table-name)))))

(defrule dbf-option (or option-batch-rows
                        option-batch-size
                        option-batch-concurrency
                        option-truncate
                        option-create-table
                        option-table-name))

(defrule another-dbf-option (and comma dbf-option)
  (:lambda (source)
    (destructuring-bind (comma option) source
      (declare (ignore comma))
      option)))

(defrule dbf-option-list (and dbf-option (* another-dbf-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist `(,opt1 ,@opts)))))

(defrule dbf-options (and kw-with dbf-option-list)
  (:lambda (source)
    (destructuring-bind (w opts) source
      (declare (ignore w))
      (cons :dbf-options opts))))

(defrule dbf-source (and kw-load kw-dbf kw-from filename-or-http-uri)
  (:lambda (src)
    (destructuring-bind (load dbf from source) src
      (declare (ignore load dbf from))
      source)))

(defrule load-dbf-optional-clauses (* (or dbf-options gucs))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule load-dbf-command (and dbf-source target load-dbf-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source target clauses) command
      `(,source ,target ,@clauses))))

(defrule load-dbf-file load-dbf-command
  (:lambda (command)
    (destructuring-bind (source pg-db-uri &key ((:dbf-options options)) gucs)
        command
      (destructuring-bind (&key dbname table-name &allow-other-keys)
	  pg-db-uri
	`(lambda ()
	   (let* ((state-before   (pgloader.utils:make-pgstate))
		  (*state*        (pgloader.utils:make-pgstate))
                  ,@(pgsql-connection-bindings pg-db-uri gucs)
                  ,@(batch-control-bindings options)
		  (source
		   ,(destructuring-bind (kind url) source
		     (ecase kind
		       (:http     `(with-stats-collection
				       ("download" :state state-before)
				     (pgloader.archive:http-fetch-file ,url)))
		       (:filename url))))
		  (source
		   (if (string= "zip" (pathname-type source))
		       (progn
			 (with-stats-collection ("extract" :state state-before)
			   (let ((d (pgloader.archive:expand-archive source)))
			     (merge-pathnames
			      (make-pathname :name (pathname-name source)
					     :type "dbf")
			      d))))
		       source))
		  (source
		   (make-instance 'pgloader.db3:copy-db3
				  :target-db ,dbname
				  :source source
				  :target ,table-name)))

	     (pgloader.sources:copy-from source
					 :state-before state-before
					 ,@(remove-batch-control-option options))

	     (report-full-summary "Total import time" *state*
				  :before state-before)))))))


#|
    LOAD IXF FROM '/Users/dim/Downloads/comsimp2013.ixf'
        INTO postgresql://dim@localhost:54393/dim?comsimp2013
        WITH truncate, create table, table name = 'comsimp2013'
|#
(defrule option-create-table (and kw-create kw-table)
  (:constant (cons :create-table t)))

;;; piggyback on DBF parsing
(defrule ixf-options (and kw-with dbf-option-list)
  (:lambda (source)
    (destructuring-bind (w opts) source
      (declare (ignore w))
      (cons :ixf-options opts))))

(defrule ixf-source (and kw-load kw-ixf kw-from filename-or-http-uri)
  (:lambda (src)
    (destructuring-bind (load ixf from source) src
      (declare (ignore load ixf from))
      source)))

(defrule load-ixf-optional-clauses (* (or ixf-options gucs))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule load-ixf-command (and ixf-source target load-ixf-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source target clauses) command
      `(,source ,target ,@clauses))))

(defrule load-ixf-file load-ixf-command
  (:lambda (command)
    (destructuring-bind (source pg-db-uri &key ((:ixf-options options)) gucs)
        command
      (destructuring-bind (&key dbname table-name &allow-other-keys)
	  pg-db-uri
	`(lambda ()
	   (let* ((state-before   (pgloader.utils:make-pgstate))
		  (*state*        (pgloader.utils:make-pgstate))
                  ,@(pgsql-connection-bindings pg-db-uri gucs)
                  ,@(batch-control-bindings options)
		  (source
		   ,(destructuring-bind (kind url) source
		     (ecase kind
		       (:http     `(with-stats-collection
				       ("download" :state state-before)
				     (pgloader.archive:http-fetch-file ,url)))
		       (:filename url))))
		  (source
		   (if (string= "zip" (pathname-type source))
		       (progn
			 (with-stats-collection ("extract" :state state-before)
			   (let ((d (pgloader.archive:expand-archive source)))
			     (merge-pathnames
			      (make-pathname :name (pathname-name source)
					     :type "ixf")
			      d))))
		       source))
		  (source
		   (make-instance 'pgloader.ixf:copy-ixf
				  :target-db ,dbname
				  :source source
				  :target ,table-name)))

	     (pgloader.sources:copy-from source
					 :state-before state-before
					 ,@(remove-batch-control-option options))

	     (report-full-summary "Total import time" *state*
				  :before state-before)))))))


#|
    LOAD CSV FROM /Users/dim/dev/CL/pgloader/galaxya/yagoa/communaute_profil.csv
        INTO postgresql://dim@localhost:54393/yagoa?commnaute_profil

        WITH truncate,
             fields optionally enclosed by '\"',
             fields escaped by \"\,
             fields terminated by '\t',
             reset sequences;

    LOAD CSV FROM '*/GeoLiteCity-Blocks.csv'
             (
                startIpNum, endIpNum, locId
             )
        INTO postgresql://dim@localhost:54393/dim?geolite.blocks
             (
                iprange ip4r using (ip-range startIpNum endIpNum),
                locId
             )
        WITH truncate,
             skip header = 2,
             fields optionally enclosed by '\"',
             fields escaped by '\"',
             fields terminated by '\t';
|#
(defrule hex-char-code (and "0x" (+ (hexdigit-char-p character)))
  (:lambda (hex)
    (destructuring-bind (prefix digits) hex
      (declare (ignore prefix))
      (code-char (parse-integer (text digits) :radix 16)))))

(defrule tab (and #\\ #\t) (:constant #\Tab))

(defrule separator (and #\' (or hex-char-code tab character ) #\')
  (:lambda (sep)
    (destructuring-bind (open char close) sep
      (declare (ignore open close))
      char)))

;;
;; Main CSV options (WITH ... in the command grammar)
;;
(defrule option-skip-header (and kw-skip kw-header equal-sign
				 (+ (digit-char-p character)))
  (:lambda (osh)
    (destructuring-bind (skip header eqs digits) osh
      (declare (ignore skip header eqs))
      (cons :skip-lines (parse-integer (text digits))))))

(defrule option-fields-enclosed-by
    (and kw-fields (? kw-optionally) kw-enclosed kw-by separator)
  (:lambda (enc)
    (destructuring-bind (f e o b sep) enc
      (declare (ignore f e o b))
      (cons :quote sep))))

(defrule option-fields-not-enclosed (and kw-fields kw-not kw-enclosed)
  (:constant (cons :quote nil)))

(defrule quote-quote     "double-quote"   (:constant "\"\""))
(defrule backslash-quote "backslash-quote" (:constant "\\\""))
(defrule escaped-quote-name    (or quote-quote backslash-quote))
(defrule escaped-quote-literal (or (and #\" #\") (and #\\ #\")) (:text t))
(defrule escaped-quote         (or escaped-quote-literal escaped-quote-name))

(defrule option-fields-escaped-by (and kw-fields kw-escaped kw-by escaped-quote)
  (:lambda (esc)
    (destructuring-bind (f e b sep) esc
      (declare (ignore f e b))
      (cons :escape sep))))

(defrule option-terminated-by (and kw-terminated kw-by separator)
  (:lambda (term)
    (destructuring-bind (terminated by sep) term
      (declare (ignore terminated by))
      (cons :separator sep))))

(defrule option-fields-terminated-by (and kw-fields option-terminated-by)
  (:lambda (term)
    (destructuring-bind (fields sep) term
      (declare (ignore fields ))
      sep)))

(defrule option-lines-terminated-by (and kw-lines kw-terminated kw-by separator)
  (:lambda (term)
    (destructuring-bind (lines terminated by sep) term
      (declare (ignore lines terminated by))
      (cons :newline sep))))

(defrule option-keep-unquoted-blanks (and kw-keep kw-unquoted kw-blanks)
  (:constant (cons :trim-blanks nil)))

(defrule option-trim-unquoted-blanks (and kw-trim kw-unquoted kw-blanks)
  (:constant (cons :trim-blanks t)))

(defrule csv-option (or option-batch-rows
                        option-batch-size
                        option-batch-concurrency
                        option-truncate
                        option-skip-header
                        option-lines-terminated-by
                        option-fields-not-enclosed
                        option-fields-enclosed-by
                        option-fields-escaped-by
                        option-fields-terminated-by
                        option-trim-unquoted-blanks
                        option-keep-unquoted-blanks))

(defrule another-csv-option (and comma csv-option)
  (:lambda (source)
    (destructuring-bind (comma option) source
      (declare (ignore comma))
      option)))

(defrule csv-option-list (and csv-option (* another-csv-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist `(,opt1 ,@opts)))))

(defrule csv-options (and kw-with csv-option-list)
  (:lambda (source)
    (destructuring-bind (w opts) source
      (declare (ignore w))
      (cons :csv-options opts))))

;;
;; CSV per-field reading options
;;
(defrule single-quoted-string (and #\' (* (not #\')) #\')
  (:lambda (qs)
    (destructuring-bind (open string close) qs
      (declare (ignore open close))
      (text string))))

(defrule double-quoted-string (and #\" (* (not #\")) #\")
  (:lambda (qs)
    (destructuring-bind (open string close) qs
      (declare (ignore open close))
      (text string))))

(defrule quoted-string (or single-quoted-string double-quoted-string))

(defrule option-date-format (and kw-date kw-format quoted-string)
  (:lambda (df)
    (destructuring-bind (date format date-format) df
      (declare (ignore date format))
      (cons :date-format date-format))))

(defrule blanks kw-blanks (:constant :blanks))

(defrule option-null-if (and kw-null kw-if (or blanks quoted-string))
  (:lambda (nullif)
    (destructuring-bind (null if opt) nullif
      (declare (ignore null if))
      (cons :null-as opt))))

(defrule csv-field-option (or option-terminated-by
			      option-date-format
			      option-null-if))

(defrule csv-field-options (* csv-field-option)
  (:lambda (options)
    (alexandria:alist-plist options)))

(defrule csv-raw-field-name (and (or #\_ (alpha-char-p character))
                                  (* (or (alpha-char-p character)
                                         (digit-char-p character)
                                         #\_)))
  (:text t))

(defrule csv-bare-field-name csv-raw-field-name
  (:lambda (name)
    (string-downcase name)))

(defrule csv-quoted-field-name (and #\" csv-raw-field-name #\")
  (:lambda (csv-field-name)
    (destructuring-bind (open name close) csv-field-name
      (declare (ignore open close))
      name)))

(defrule csv-field-name (or csv-quoted-field-name csv-bare-field-name))

(defrule csv-source-field (and csv-field-name csv-field-options)
  (:destructure (name opts)
    `(,name ,@opts)))

(defrule another-csv-source-field (and comma csv-source-field)
  (:lambda (source)
    (destructuring-bind (comma field) source
      (declare (ignore comma))
      field)))

(defrule csv-source-fields (and csv-source-field (* another-csv-source-field))
  (:lambda (source)
    (destructuring-bind (field1 fields) source
      (list* field1 fields))))

(defrule open-paren (and ignore-whitespace #\( ignore-whitespace)
  (:constant :open-paren))
(defrule close-paren (and ignore-whitespace #\) ignore-whitespace)
  (:constant :close-paren))

(defrule having-fields (and kw-having kw-fields) (:constant nil))

(defrule csv-source-field-list (and (? having-fields)
                                    open-paren csv-source-fields close-paren)
  (:lambda (source)
    (destructuring-bind (having open field-defs close) source
      (declare (ignore having open close))
      field-defs)))

;;
;; csv-target-column-list
;;
;;      iprange ip4r using (ip-range startIpNum endIpNum),
;;
(defrule column-name csv-field-name)	; same rules here
(defrule column-type csv-field-name)	; again, same rules, names only

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
    (pgloader.transforms:intern-symbol (text schars))))

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
			(or sexp-string sexp-integer sexp-symbol))
  (:lambda (atom)
    (destructuring-bind (ws a) atom
      (declare (ignore ws))
      a)))

(defrule sexp (or sexp-atom sexp-list))

(defrule column-expression (and kw-using sexp)
  (:lambda (expr)
    (destructuring-bind (using sexp) expr
      (declare (ignore using))
      sexp)))

(defrule csv-target-column (and column-name
				(? (and ignore-whitespace column-type
					column-expression)))
  (:lambda (col)
    (destructuring-bind (name opts) col
      (if opts
	  (destructuring-bind (ws type expr) opts
	    (declare (ignore ws))
	    (list name type expr))
	  (list name nil nil)))))

(defrule another-csv-target-column (and comma csv-target-column)
  (:lambda (source)
    (destructuring-bind (comma col) source
      (declare (ignore comma))
      col)))

(defrule csv-target-columns (and csv-target-column
				 (* another-csv-target-column))
  (:lambda (source)
    (destructuring-bind (col1 cols) source
      (list* col1 cols))))

(defrule target-columns (and kw-target kw-columns) (:constant nil))

(defrule csv-target-column-list (and (? target-columns)
                                     open-paren csv-target-columns close-paren)
  (:lambda (source)
    (destructuring-bind (target-columns open columns close) source
      (declare (ignore target-columns open close))
      columns)))
;;
;; The main command parsing
;;
(defun find-encoding-by-name-or-alias (encoding)
  "charsets::*lisp-encodings* is an a-list of (NAME . ALIASES)..."
  (loop :for (name . aliases) :in (list-encodings-and-aliases)
     :for encoding-name := (when (or (string-equal name encoding)
                                     (member encoding aliases :test #'string-equal))
                             name)
     :until encoding-name
     :finally (if encoding-name (return encoding-name)
                  (error "The encoding '~a' is unknown" encoding))))

(defrule encoding (or namestring single-quoted-string)
  (:lambda (encoding)
    (make-external-format (find-encoding-by-name-or-alias encoding))))

(defrule file-encoding (? (and kw-with kw-encoding encoding))
  (:lambda (enc)
    (if enc
	(destructuring-bind (with kw-encoding encoding) enc
	  (declare (ignore with kw-encoding))
	  encoding)
	:utf-8)))

(defrule first-filename-matching
    (and (? kw-first) kw-filename kw-matching quoted-regex)
  (:lambda (fm)
    (destructuring-bind (first filename matching regex) fm
      (declare (ignore first filename matching))
      ;; regex is a list with first the symbol :regex and second the regexp
      ;; as a string
      (list* :regex :first (cdr regex)))))

(defrule all-filename-matching
    (and kw-all (or kw-filenames kw-filename) kw-matching quoted-regex)
  (:lambda (fm)
    (destructuring-bind (all filename matching regex) fm
      (declare (ignore all filename matching))
      ;; regex is a list with first the symbol :regex and second the regexp
      ;; as a string
      (list* :regex :all (cdr regex)))))

(defrule in-directory (and kw-in kw-directory maybe-quoted-filename)
  (:lambda (in-d)
    (destructuring-bind (in d dir) in-d
      (declare (ignore in d))
      dir)))

(defrule filename-matching (and (or first-filename-matching
                                    all-filename-matching)
                                (? in-directory))
  (:lambda (filename-matching)
    (destructuring-bind (matching directory) filename-matching
      (let ((directory (or directory `(:filename ,*cwd*))))
        (destructuring-bind (m-type first-or-all regex) matching
          (assert (eq m-type :regex))
          (destructuring-bind (d-type dir) directory
            (assert (eq d-type :filename))
            (let ((root (uiop:directory-exists-p
                         (if (uiop:absolute-pathname-p dir) dir
                             (uiop:merge-pathnames* dir *cwd*)))))
              (unless root
                (error "Directory ~s does not exists."
                       (uiop:native-namestring dir)))
             `(:regex ,first-or-all ,regex ,root))))))))

(defrule csv-file-source (or stdin
			     inline
			     filename-matching
			     maybe-quoted-filename))

(defrule csv-source (and kw-load kw-csv kw-from csv-file-source)
  (:lambda (src)
    (destructuring-bind (load csv from source) src
      (declare (ignore load csv from))
      ;; source is (:filename #P"pathname/here")
      (destructuring-bind (type &rest data) source
	(declare (ignore data))
	(ecase type
	  (:stdin    source)
	  (:inline   source)
	  (:filename source)
	  (:regex    source))))))

(defun list-symbols (expression &optional s)
  "Return a list of the symbols used in EXPRESSION."
  (typecase expression
    (symbol  (pushnew expression s))
    (list    (loop for e in expression for s = (list-symbols e s)
		finally (return (reverse s))))
    (t       s)))


;;;
;;; The main CSV loading command, with optional clauses
;;;
(defrule load-csv-file-optional-clauses (* (or csv-options
                                               gucs
                                               before-load
                                               after-load))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule load-csv-file-command (and csv-source
                                    (? file-encoding) (? csv-source-field-list)
                                    target (? csv-target-column-list)
                                    load-csv-file-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source encoding fields target columns clauses) command
      `(,source ,encoding ,fields ,target ,columns ,@clauses))))

(defrule load-csv-file load-csv-file-command
  (:lambda (command)
    (destructuring-bind (source encoding fields pg-db-uri columns
                                &key ((:csv-options options)) gucs before after)
        command
      (destructuring-bind (&key dbname table-name &allow-other-keys)
	  pg-db-uri
	`(lambda ()
	   (let* ((state-before  ,(when before `(pgloader.utils:make-pgstate)))
		  (summary       (null *state*))
		  (*state*       (or *state* (pgloader.utils:make-pgstate)))
		  (state-after   ,(when after `(pgloader.utils:make-pgstate)))
                  ,@(pgsql-connection-bindings pg-db-uri gucs)
                  ,@(batch-control-bindings options))

	     (progn
	       ,(sql-code-block dbname 'state-before before "before load")

	       (let ((truncate (getf ',options :truncate))
		     (source
		      (make-instance 'pgloader.csv:copy-csv
				     :target-db  ,dbname
				     :source    ',source
				     :target     ,table-name
				     :encoding   ,encoding
				     :fields    ',fields
				     :columns   ',columns
                                     ,@(remove-batch-control-option
                                        options :extras '(:truncate)))))
		 (pgloader.sources:copy-from source :truncate truncate))

	       ,(sql-code-block dbname 'state-after after "after load")

	       ;; reporting
	       (when summary
		 (report-full-summary "Total import time" *state*
				      :before  state-before
				      :finally state-after)))))))))


;;;
;;; LOAD FIXED COLUMNS FILE
;;;
;;; That has lots in common with CSV, so we share a fair amount of parsing
;;; rules with the CSV case.
;;;
(defrule hex-number (and "0x" (+ (hexdigit-char-p character)))
  (:lambda (hex)
    (destructuring-bind (prefix digits) hex
      (declare (ignore prefix))
      (parse-integer (text digits) :radix 16))))

(defrule dec-number (+ (digit-char-p character))
  (:lambda (digits)
    (parse-integer (text digits))))

(defrule number (or hex-number dec-number))

(defrule field-start-position (and ignore-whitespace number)
  (:destructure (ws pos) (declare (ignore ws)) pos))

(defrule fixed-field-length (and ignore-whitespace number)
  (:destructure (ws len) (declare (ignore ws)) len))

(defrule fixed-source-field (and csv-field-name
				 field-start-position fixed-field-length
				 csv-field-options)
  (:destructure (name start len opts)
    `(,name :start ,start :length ,len ,@opts)))

(defrule another-fixed-source-field (and comma fixed-source-field)
  (:lambda (source)
    (destructuring-bind (comma field) source
      (declare (ignore comma))
      field)))

(defrule fixed-source-fields (and fixed-source-field (* another-fixed-source-field))
  (:lambda (source)
    (destructuring-bind (field1 fields) source
      (list* field1 fields))))

(defrule fixed-source-field-list (and open-paren fixed-source-fields close-paren)
  (:lambda (source)
    (destructuring-bind (open field-defs close) source
      (declare (ignore open close))
      field-defs)))

(defrule fixed-option (or option-batch-rows
                          option-batch-size
                          option-batch-concurrency
                          option-truncate
			  option-skip-header))

(defrule another-fixed-option (and comma fixed-option)
  (:lambda (source)
    (destructuring-bind (comma option) source
      (declare (ignore comma))
      option)))

(defrule fixed-option-list (and fixed-option (* another-fixed-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist `(,opt1 ,@opts)))))

(defrule fixed-options (and kw-with csv-option-list)
  (:lambda (source)
    (destructuring-bind (w opts) source
      (declare (ignore w))
      (cons :fixed-options opts))))


(defrule fixed-file-source (or stdin
			       inline
			       filename-matching
			       maybe-quoted-filename))

(defrule fixed-source (and kw-load kw-fixed kw-from fixed-file-source)
  (:lambda (src)
    (destructuring-bind (load fixed from source) src
      (declare (ignore load fixed from))
      ;; source is (:filename #P"pathname/here")
      (destructuring-bind (type &rest data) source
	(declare (ignore data))
	(ecase type
	  (:stdin    source)
	  (:inline   source)
	  (:filename source)
	  (:regex    source))))))

(defrule load-fixed-cols-file-optional-clauses (* (or fixed-options
                                                      gucs
                                                      before-load
                                                      after-load))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule load-fixed-cols-file-command (and fixed-source (? file-encoding)
                                           fixed-source-field-list
                                           target
                                           (? csv-target-column-list)
                                           load-fixed-cols-file-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source encoding fields target columns clauses) command
      `(,source ,encoding ,fields ,target ,columns ,@clauses))))

(defrule load-fixed-cols-file load-fixed-cols-file-command
  (:lambda (command)
    (destructuring-bind (source encoding fields pg-db-uri columns
                                &key ((:fixed-options options)) gucs before after)
        command
      (destructuring-bind (&key dbname table-name &allow-other-keys)
	  pg-db-uri
	`(lambda ()
	   (let* ((state-before  ,(when before `(pgloader.utils:make-pgstate)))
		  (summary       (null *state*))
		  (*state*       (or *state* (pgloader.utils:make-pgstate)))
		  (state-after   ,(when after `(pgloader.utils:make-pgstate)))
                  ,@(pgsql-connection-bindings pg-db-uri gucs)
                  ,@(batch-control-bindings options))

	     (progn
	       ,(sql-code-block dbname 'state-before before "before load")

	       (let ((truncate ,(getf options :truncate))
		     (source
		      (make-instance 'pgloader.fixed:copy-fixed
				     :target-db ,dbname
				     :source ',source
				     :target ,table-name
				     :encoding ,encoding
				     :fields ',fields
				     :columns ',columns
				     :skip-lines ,(or (getf options :skip-line) 0))))
		 (pgloader.sources:copy-from source :truncate truncate))

	       ,(sql-code-block dbname 'state-after after "after load")

	       ;; reporting
	       (when summary
		 (report-full-summary "Total import time" *state*
				      :before  state-before
				      :finally state-after)))))))))


;;;
;;; LOAD ARCHIVE ...
;;;
(defrule archive-command (or load-csv-file
			     load-dbf-file
			     load-fixed-cols-file))

(defrule another-archive-command (and kw-and archive-command)
  (:lambda (source)
    (destructuring-bind (and col) source
      (declare (ignore and))
      col)))

(defrule archive-command-list (and archive-command (* another-archive-command))
  (:lambda (source)
    (destructuring-bind (col1 cols) source
      (cons :commands (list* col1 cols)))))

(defrule filename-or-http-uri (or http-uri maybe-quoted-filename))

(defrule archive-source (and kw-load kw-archive kw-from filename-or-http-uri)
  (:lambda (src)
    (destructuring-bind (load from archive source) src
      (declare (ignore load from archive))
      source)))

(defrule load-archive-clauses (and archive-source
                                   (? target)
                                   (? before-load)
                                   archive-command-list
                                   (? finally))
  (:lambda (command)
    (destructuring-bind (source target before commands finally) command
      (destructuring-bind (&key before commands finally)
          (alexandria:alist-plist (list before commands finally))
        (list source target
              :before before
              :commands commands
              :finally finally)))))

(defrule load-archive load-archive-clauses
  (:lambda (archive)
    (destructuring-bind (source pg-db-uri &key before commands finally) archive
      (when (and (or before finally) (null pg-db-uri))
	(error "When using a BEFORE LOAD DO or a FINALLY block, you must provide an archive level target database connection."))
      (destructuring-bind (&key host port user password dbname &allow-other-keys)
	  pg-db-uri
	`(lambda ()
	   (let* ((state-before   (pgloader.utils:make-pgstate))
		  (*state*        (pgloader.utils:make-pgstate))
                  (*pgconn-host* ',host)
		  (*pgconn-port* ,port)
		  (*pgconn-user* ,user)
		  (*pgconn-pass* ,password)
		  (*pg-dbname*   ,dbname)
		  (state-finally ,(when finally `(pgloader.utils:make-pgstate)))
		  (archive-file
		   ,(destructuring-bind (kind url) source
		     (ecase kind
		       (:http     `(with-stats-collection
				       ("download" :state state-before)
				     (pgloader.archive:http-fetch-file ,url)))
		       (:filename url))))
		  (*csv-path-root*
		   (with-stats-collection ("extract" :state state-before)
		     (pgloader.archive:expand-archive archive-file))))
	     (progn
	       ,(sql-code-block dbname 'state-before before "before load")

	       ;; import from files block
	       ,@(loop for command in commands
		    collect `(funcall ,command))

	       ,(sql-code-block dbname 'state-finally finally "finally")

	       ;; reporting
	       (report-full-summary "Total import time" *state*
				    :before state-before
				    :finally state-finally))))))))


;;;
;;; Now the main command, one of
;;;
;;;  - LOAD FROM some files
;;;  - LOAD DATABASE FROM a MySQL remote database
;;;  - LOAD MESSAGES FROM a syslog daemon receiver we're going to start here
;;;
(defrule end-of-command (and ignore-whitespace #\; ignore-whitespace)
  (:constant :eoc))

(defrule command (and (or load-archive
			  load-csv-file
			  load-fixed-cols-file
			  load-dbf-file
                          load-ixf-file
			  load-mysql-database
			  load-sqlite-database
			  load-syslog-messages)
		      end-of-command)
  (:lambda (cmd)
    (destructuring-bind (command eoc) cmd
      (declare (ignore eoc))
      command)))

(defrule commands (+ command))

(defun parse-commands (commands)
  "Parse a command and return a LAMBDA form that takes no parameter."
  (parse 'commands commands))

(defun inject-inline-data-position (command position)
  "We have '(:inline nil) somewhere in command, have '(:inline position) instead."
  (loop
     for s-exp in command
     when (equal '(:inline nil) s-exp) collect (list :inline position)
     else collect (if (and (consp s-exp) (listp (cdr s-exp)))
		      (inject-inline-data-position s-exp position)
		      s-exp)))

(defun process-relative-pathnames (filename command)
  "Walk the COMMAND to replace relative pathname with absolute ones, merging
   them within the directory where we found the command FILENAME."
  (loop
     for s-exp in command
     when (pathnamep s-exp)
     collect (if (fad:pathname-relative-p s-exp)
		 (merge-pathnames s-exp (directory-namestring filename))
		 s-exp)
     else
     collect (if (and (consp s-exp) (listp (cdr s-exp)))
		 (process-relative-pathnames filename s-exp)
		 s-exp)))

(defun parse-commands-from-file (maybe-relative-filename
                                 &aux (filename
                                       ;; we want a truename here
                                       (probe-file maybe-relative-filename)))
  "The command could be using from :inline, in which case we want to parse
   as much as possible then use the command against an already opened stream
   where we moved at the beginning of the data."
  (if filename
      (log-message :log "Parsing commands from file ~s~%" filename)
      (error "Can not find file: ~s" maybe-relative-filename))

  (process-relative-pathnames
   filename
   (let ((*cwd* (directory-namestring filename))
         (*data-expected-inline* nil)
	 (content (slurp-file-into-string filename)))
     (multiple-value-bind (commands end-commands-position)
	 (parse 'commands content :junk-allowed t)

       ;; INLINE is only allowed where we have a single command in the file
       (if *data-expected-inline*
	   (progn
	     (when (= 0 end-commands-position)
	       ;; didn't find any command, leave error reporting to esrap
	       (parse 'commands content))

	     (when (and *data-expected-inline*
			(null end-commands-position))
	       (error "Inline data not found in '~a'." filename))

	     (when (and *data-expected-inline* (not (= 1 (length commands))))
	       (error (concatenate 'string
				   "Too many commands found in '~a'.~%"
				   "To use inline data, use a single command.")
		      filename))

	     ;; now we should have a single command and inline data after that
	     ;; replace the (:inline nil) found in the first (and only) command
	     ;; with a (:inline position) instead
	     (list
	      (inject-inline-data-position
	       (first commands) (cons filename end-commands-position))))

	   ;; There was no INLINE magic found in the file, reparse it so that
	   ;; normal error processing happen
	   (parse 'commands content))))))

(defun run-commands (source
		     &key
		       (start-logger t)
                       ((:summary summary-pathname))
		       ((:log-filename *log-filename*) *log-filename*)
		       ((:log-min-messages *log-min-messages*) *log-min-messages*)
		       ((:client-min-messages *client-min-messages*) *client-min-messages*))
  "SOURCE can be a function, which is run, a list, which is compiled as CL
   code then run, a pathname containing one or more commands that are parsed
   then run, or a commands string that is then parsed and each command run."

  (with-monitor (:start-logger start-logger)
    (let* ((funcs
            (typecase source
              (function (list source))

              (list     (list (compile nil source)))

              (pathname (mapcar (lambda (expr) (compile nil expr))
                                (parse-commands-from-file source)))

              (t        (mapcar (lambda (expr) (compile nil expr))
                                (if (probe-file source)
                                    (parse-commands-from-file source)
                                    (parse-commands source)))))))

      ;; maybe duplicate the summary to a file
      (let* ((summary-stream (when summary-pathname
                              (open summary-pathname
                                    :direction :output
                                    :if-exists :rename
                                    :if-does-not-exist :create)))
             (*report-stream* (apply
                               #'make-broadcast-stream
                               (remove-if #'null (list *terminal-io*
                                                       summary-stream)))))
        (unwind-protect
             ;; run the commands
             (loop for func in funcs do (funcall func))

          ;; cleanup
          (when summary-stream (close summary-stream)))))))


;;;
;;; Interactive tool
;;;
(defmacro with-database-uri ((database-uri) &body body)
  "Run the BODY forms with the connection parameters set to proper values
   from the DATABASE-URI. For a MySQL connection string, that's
   *myconn-user* and all, for a PostgreSQL connection string, *pgconn-user*
   and all."
  (destructuring-bind (&key type user password host port dbname
                            &allow-other-keys)
      (parse 'db-connection-uri database-uri)
    (ecase type
      (:mysql
       `(let* ((*myconn-host* ,(if (consp host) (list 'quote host) host))
	       (*myconn-port* ,port)
	       (*myconn-user* ,user)
	       (*myconn-pass* ,password)
               (*my-dbname*   ,dbname))
	  ,@body))
      (:postgresql
       `(let* ((*pgconn-host* ,(if (consp host) (list 'quote host) host))
	       (*pgconn-port* ,port)
	       (*pgconn-user* ,user)
	       (*pgconn-pass* ,password)
               (*pg-dbname*   ,dbname))
	  ,@body)))))


;;;
;;; Some testing
;;;
(defun test-parsing (&rest tests)
  "Try parsing the command(s) from the file test/TEST.load"
  (let* ((tdir  (directory-namestring
		 (asdf:system-relative-pathname :pgloader "test/")))
	 (tests (or (remove-if #'null tests) (fad:list-directory tdir))))
    (loop
       for test in tests
       for filename =
	 (if (fad:pathname-relative-p test)
	     (make-pathname :directory tdir :name test :type "load")
	     test)
       collect
	 (cons filename
	       (ignore-errors
		 (parse-commands (slurp-file-into-string filename)))))))

(defun list-failing-tests (&rest tests)
  "Return the list of test files we can't parse."
  (loop for (name . code) in (test-parsing tests) unless code collect name))



;;;
;;; Parse the pgloader commands grammar
;;;

(in-package :pgloader.parser)

(defparameter *default-host* "localhost")
(defparameter *default-postgresql-port* 5432)
(defparameter *default-mysql-port* 3306)

(defvar *data-expected-inline* nil
  "Set to :inline when parsing an INLINE keyword in a FROM clause.")

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
(defrule keep-a-single-whitespace (+ (or #\space #\tab #\newline #\linefeed))
  (:constant " "))

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
  (def-keyword-rule "into")
  (def-keyword-rule "with")
  (def-keyword-rule "when")
  (def-keyword-rule "set")
  (def-keyword-rule "database")
  (def-keyword-rule "messages")
  (def-keyword-rule "matches")
  (def-keyword-rule "in")
  (def-keyword-rule "registering")
  (def-keyword-rule "cast")
  (def-keyword-rule "column")
  (def-keyword-rule "type")
  (def-keyword-rule "extra")
  (def-keyword-rule "drop")
  (def-keyword-rule "not")
  (def-keyword-rule "to")
  (def-keyword-rule "null")
  (def-keyword-rule "default")
  (def-keyword-rule "using")
  ;; option for loading from a file
  (def-keyword-rule "workers")
  (def-keyword-rule "batch")
  (def-keyword-rule "size")
  (def-keyword-rule "reject")
  (def-keyword-rule "file")
  (def-keyword-rule "log")
  (def-keyword-rule "level")
  (def-keyword-rule "encoding")
  (def-keyword-rule "truncate")
  (def-keyword-rule "lines")
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
  (def-keyword-rule "blanks")
  (def-keyword-rule "date")
  (def-keyword-rule "format")
  ;; option for MySQL imports
  (def-keyword-rule "schema")
  (def-keyword-rule "only")
  (def-keyword-rule "drop")
  (def-keyword-rule "create")
  (def-keyword-rule "reset")
  (def-keyword-rule "table")
  (def-keyword-rule "name")
  (def-keyword-rule "tables")
  (def-keyword-rule "indexes")
  (def-keyword-rule "sequences")
  (def-keyword-rule "downcase")
  (def-keyword-rule "quote")
  (def-keyword-rule "identifiers")
  ;; option for loading from an archive
  (def-keyword-rule "archive")
  (def-keyword-rule "before")
  (def-keyword-rule "after")
  (def-keyword-rule "finally")
  (def-keyword-rule "and")
  (def-keyword-rule "do")
  (def-keyword-rule "filename")
  (def-keyword-rule "matching"))

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
      (list :type type
	    :user user
	    :password password
	    :host (or host *default-host*)
	    :port (or port (case type
			     (:postgresql *default-postgresql-port*)
			     (:mysql      *default-mysql-port*)))
	    :dbname dbname
	    :table-name table-name))))

(defrule target (and kw-into db-connection-uri)
  (:destructure (into target)
    (declare (ignore into))
    (destructuring-bind (&key type &allow-other-keys) target
      (unless (eq type :postgresql)
	(error "The target must be a PostgreSQL connection string."))
      target)))


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
	 (pgloader.pgsql:copy-from-file ,dbname ,table-name ',source)))))

(defrule database-source (and ignore-whitespace
			      kw-load kw-database kw-from
			      db-connection-uri)
  (:lambda (source)
    (destructuring-bind (nil l d f uri) source
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

(defrule option-drop-tables (and kw-drop kw-tables)
  (:constant (cons :include-drop t)))

(defrule option-truncate (and kw-truncate)
  (:constant (cons :truncate t)))

(defrule option-schema-only (and kw-schema kw-only)
  (:constant (cons :schema-only t)))

(defrule option-create-tables (and kw-create kw-tables)
  (:constant (cons :create-tables t)))

(defrule option-create-indexes (and kw-create kw-indexes)
  (:constant (cons :create-indexes t)))

(defrule option-reset-sequences (and kw-reset kw-sequences)
  (:constant (cons :reset-sequences t)))

(defrule option-identifiers-case (and (or kw-downcase kw-quote) kw-identifiers)
  (:lambda (id-case)
    (destructuring-bind (action id) id-case
      (declare (ignore id))
      (cons :identifier-case action))))

(defrule mysql-option (or option-workers
			  option-truncate
			  option-schema-only
			  option-drop-tables
			  option-create-tables
			  option-create-indexes
			  option-reset-sequences
			  option-identifiers-case))

(defrule another-mysql-option (and #\, ignore-whitespace mysql-option)
  (:lambda (source)
    (destructuring-bind (comma ws option) source
      (declare (ignore comma ws))
      option)))

(defrule mysql-option-list (and mysql-option (* another-mysql-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist (list* opt1 opts)))))

(defrule mysql-options (and kw-with mysql-option-list)
  (:lambda (source)
    (destructuring-bind (w opts) source
      (declare (ignore w))
      opts)))

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

(defrule another-generic-option (and #\, ignore-whitespace generic-option)
  (:lambda (source)
    (destructuring-bind (comma ws option) source
      (declare (ignore comma ws))
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
      gucs)))


;;;
;;; Now parsing CAST rules for migrating from MySQL
;;;

;; at the moment we only know about extra auto_increment
(defrule cast-source-extra (and ignore-whitespace
				kw-with kw-extra kw-auto-increment)
  (:constant (list :auto-increment t)))

(defrule cast-source (and (or kw-column kw-type)
			  trimmed-name
			  (? cast-source-extra)
			  ignore-whitespace)
  (:lambda (source)
    (destructuring-bind (kw name opts ws) source
      (declare (ignore ws))
      (destructuring-bind (&key auto-increment &allow-other-keys) opts
	(list kw name :auto-increment auto-increment)))))

(defrule cast-type-name (and (alpha-char-p character)
			     (* (or (alpha-char-p character)
				    (digit-char-p character))))
  (:text t))

(defrule cast-to-type (and kw-to cast-type-name ignore-whitespace)
  (:lambda (source)
    (destructuring-bind (to type-name ws) source
      (declare (ignore to ws))
      (list :type type-name))))

(defrule cast-drop-default  (and kw-drop kw-default)
  (:constant (list :drop-default t)))

(defrule cast-drop-not-null (and kw-drop kw-not kw-null)
  (:constant (list :drop-not-null t)))

(defrule cast-def (+ (or cast-to-type
			 cast-drop-default
			 cast-drop-not-null))
  (:lambda (source)
    (destructuring-bind
	  (&key type drop-default drop-not-null &allow-other-keys)
	(apply #'append source)
      (list :type type :drop-default drop-default :drop-not-null drop-not-null))))

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

(defrule cast-rule (and cast-source cast-def (? cast-function))
  (:lambda (cast)
    (destructuring-bind (source target function) cast
      (list :source source
	    :target (fix-target-type source target)
	    :using function))))

(defrule another-cast-rule (and #\, ignore-whitespace cast-rule)
  (:lambda (source)
    (destructuring-bind (comma ws rule) source
      (declare (ignore comma ws))
      rule)))

(defrule cast-rule-list (and cast-rule (* another-cast-rule))
  (:lambda (source)
    (destructuring-bind (rule1 rules) source
      (list* rule1 rules))))

(defrule casts (and kw-cast cast-rule-list)
  (:lambda (source)
    (destructuring-bind (c casts) source
      (declare (ignore c))
      casts)))

;;; LOAD DATABASE FROM mysql://
(defrule load-database (and database-source target
			    (? mysql-options)
			    (? gucs)
			    (? casts))
  (:lambda (source)
    (destructuring-bind (my-db-uri pg-db-uri options gucs casts) source
      (destructuring-bind (&key ((:host myhost))
				((:port myport))
				((:user myuser))
				((:password mypass))
				((:dbname mydb))
				table-name
				&allow-other-keys)
	  my-db-uri
	(destructuring-bind (&key ((:host pghost))
				  ((:port pgport))
				  ((:user pguser))
				  ((:password pgpass))
				  ((:dbname pgdb))
				  &allow-other-keys)
	    pg-db-uri
	  `(lambda ()
	     (let* ((pgloader.mysql:*cast-rules* ',casts)
		    (*myconn-host* ,myhost)
		    (*myconn-port* ,myport)
		    (*myconn-user* ,myuser)
		    (*myconn-pass* ,mypass)
		    (*pgconn-host* ,pghost)
		    (*pgconn-port* ,pgport)
		    (*pgconn-user* ,pguser)
		    (*pgconn-pass* ,pgpass)
		    (*pg-settings* ',gucs)
		    (pgloader.mysql::*pgsql-reserved-keywords*
		     (pgloader.pgsql:list-reserved-keywords ,pgdb)))
	       (declare (special pgloader.mysql:*cast-rules*
				 *myconn-host* *myconn-port*
				 *myconn-user* *myconn-pass*
				 *pgconn-host* *pgconn-port*
				 *pgconn-user* *pgconn-pass*))
	       (pgloader.mysql:stream-database ,mydb
					       ,@(when table-name
						 `(:only-tables ',(list table-name)))
					       :pg-dbname ,pgdb
					       ,@options))))))))


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

(defrule dbf-option (or option-truncate option-create-table option-table-name))

(defrule another-dbf-option (and #\, ignore-whitespace dbf-option)
  (:lambda (source)
    (destructuring-bind (comma ws option) source
      (declare (ignore comma ws))
      option)))

(defrule dbf-option-list (and dbf-option (* another-dbf-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist `(,opt1 ,@opts)))))

(defrule dbf-options (and kw-with dbf-option-list)
  (:lambda (source)
    (destructuring-bind (w opts) source
      (declare (ignore w))
      opts)))

(defrule dbf-source (and kw-load kw-dbf kw-from maybe-quoted-filename)
  (:lambda (src)
    (destructuring-bind (load dbf from source) src
      (declare (ignore load dbf from))
      ;; source is (:filename #P"pathname/here")
      (destructuring-bind (type uri) source
	(ecase type
	  (:filename uri))))))

(defrule load-dbf-file (and dbf-source target dbf-options)
  (:lambda (command)
    (destructuring-bind (source pg-db-uri options) command
      (destructuring-bind (&key host port user password dbname table-name
				&allow-other-keys)
	  pg-db-uri
	`(lambda ()
	   (let* ((*pgconn-host* ,host)
		  (*pgconn-port* ,port)
		  (*pgconn-user* ,user)
		  (*pgconn-pass* ,password))
	     (pgloader.db3:stream-file ,source
				       :dbname ,dbname
				       ,@(when table-name
					       (list :table-name table-name))
				       ,@options)))))))


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

(defrule another-dollar-quoted (and ignore-whitespace #\, dollar-quoted)
  (:lambda (source)
    (destructuring-bind (ws comma quoted) source
      (declare (ignore ws comma))
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

(defrule finally-do (and kw-finally kw-do dollar-quoted-list)
  (:lambda (fd)
    (destructuring-bind (finally do quoted) fd
      (declare (ignore finally do))
      quoted)))

(defrule after-load-do (and kw-after kw-load kw-do dollar-quoted-list)
  (:lambda (fd)
    (destructuring-bind (after load do quoted) fd
      (declare (ignore after load do))
      quoted)))


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
(defun hexdigit-char-p (character)
  (member character #. (quote (coerce "0123456789abcdefABCDEF" 'list))))

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

(defrule csv-option (or option-truncate
			option-skip-header
			option-fields-not-enclosed
			option-fields-enclosed-by
			option-fields-escaped-by
			option-fields-terminated-by))

(defrule another-csv-option (and #\, ignore-whitespace csv-option)
  (:lambda (source)
    (destructuring-bind (comma ws option) source
      (declare (ignore comma ws))
      option)))

(defrule csv-option-list (and csv-option (* another-csv-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist `(,opt1 ,@opts)))))

(defrule csv-options (and kw-with csv-option-list)
  (:lambda (source)
    (destructuring-bind (w opts) source
      (declare (ignore w))
      opts)))

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

(defrule csv-field-name (and (alpha-char-p character)
			     (* (or (alpha-char-p character)
				    (digit-char-p character)
				    #\_)))
  (:text t))

(defrule csv-source-field (and csv-field-name csv-field-options)
  (:destructure (name opts)
    `(,name ,@opts)))

(defrule another-csv-source-field (and #\, ignore-whitespace csv-source-field)
  (:lambda (source)
    (destructuring-bind (comma ws field) source
      (declare (ignore comma ws))
      field)))

(defrule csv-source-fields (and csv-source-field (* another-csv-source-field))
  (:lambda (source)
    (destructuring-bind (field1 fields) source
      (list* field1 fields))))

(defrule open-paren (and ignore-whitespace #\( ignore-whitespace)
  (:constant :open-paren))
(defrule close-paren (and ignore-whitespace #\) ignore-whitespace)
  (:constant :close-paren))

(defrule csv-source-field-list (and open-paren csv-source-fields close-paren)
  (:lambda (source)
    (destructuring-bind (open field-defs close) source
      (declare (ignore open close))
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
  (or (alphanumericp character)
      (member character '(#\_ #\-))))

(defrule sexp-symbol (+ (symbol-character-p character))
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

(defrule another-csv-target-column (and #\, ignore-whitespace csv-target-column)
  (:lambda (source)
    (destructuring-bind (comma ws col) source
      (declare (ignore comma ws))
      col)))

(defrule csv-target-columns (and csv-target-column
				 (* another-csv-target-column))
  (:lambda (source)
    (destructuring-bind (col1 cols) source
      (list* col1 cols))))

(defrule csv-target-column-list (and open-paren csv-target-columns close-paren)
  (:lambda (source)
    (destructuring-bind (open columns close) source
      (declare (ignore open close))
      columns)))
;;
;; The main command parsing
;;
(defun find-encoding-by-name-or-alias (encoding)
  "charsets::*lisp-encodings* is an a-list of (NAME . ALIASES)..."
  (loop for (name . aliases) in charsets::*lisp-encodings*
     for encoding-name = (when (or (string-equal name encoding)
				   (member encoding aliases :test #'string-equal))
			   name)
     until encoding-name
     finally (if encoding-name (return encoding-name)
		 (error "The encoding '~a' is unknown" encoding))))

(defrule encoding namestring
  (:lambda (encoding)
    (charsets:make-external-format (find-encoding-by-name-or-alias encoding))))

(defrule file-encoding (? (and kw-with kw-encoding encoding))
  (:lambda (enc)
    (if enc
	(destructuring-bind (with kw-encoding encoding) enc
	  (declare (ignore with kw-encoding))
	  encoding)
	:utf-8)))

(defrule filename-matching (and kw-filename kw-matching quoted-regex)
  (:lambda (fm)
    (destructuring-bind (filename matching regex) fm
      (declare (ignore filename matching))
      regex)))

(defrule csv-file-source (or stdin
			     inline
			     filename-matching
			     maybe-quoted-filename))

(defrule csv-source (and kw-load kw-csv kw-from csv-file-source)
  (:lambda (src)
    (destructuring-bind (load csv from source) src
      (declare (ignore load csv from))
      ;; source is (:filename #P"pathname/here")
      (destructuring-bind (type uri) source
	(declare (ignore uri))
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

(defrule load-csv-file (and csv-source (? file-encoding) (? csv-source-field-list)
			    target (? csv-target-column-list)
			    csv-options
			    (? before-load-do)
			    (? after-load-do))
  (:lambda (command)
    (destructuring-bind (source encoding fields pg-db-uri
				columns options before after) command
      (destructuring-bind (&key host port user password dbname table-name
				&allow-other-keys)
	  pg-db-uri
	`(lambda ()
	   (let* ((state-before  (when ',before (pgloader.utils:make-pgstate)))
		  (summary       (null *state*))
		  (*state*       (or *state* (pgloader.utils:make-pgstate)))
		  (state-after   (when ',after (pgloader.utils:make-pgstate)))
		  (*pgconn-host* ,host)
		  (*pgconn-port* ,port)
		  (*pgconn-user* ,user)
		  (*pgconn-pass* ,password))

	     ;; before block
	     ,(when before
	       `(with-stats-collection (,dbname "before load" :state state-before)
		  (with-pgsql-transaction (,dbname)
		    (loop for command in ',before
		       do
			 (log-message :notice command)
			 (pgsql-execute command :client-min-messages :error)))))

	     (pgloader.csv:copy-from-file ,dbname
					  ,table-name
					  ',source
					  :encoding ,encoding
					  :fields ',fields
					  :columns ',columns
					  ,@options)
	     ;; finally block
	     ,(when after
	       `(with-stats-collection (,dbname "after load" :state state-after)
		  (with-pgsql-transaction (,dbname)
		    (loop for command in ',after
		       do
			 (log-message :notice command)
			 (pgsql-execute command :client-min-messages :error)))))

	     ;; reporting
	     (when summary
	       (report-full-summary *state* state-before state-after
				    "Total import time"))))))))


;;;
;;; LOAD ARCHIVE ...
;;;
(defrule archive-command (or load-csv-file
			     load-dbf-file))

(defrule another-archive-command (and kw-and archive-command)
  (:lambda (source)
    (destructuring-bind (and col) source
      (declare (ignore and))
      col)))

(defrule archive-command-list (and archive-command (* another-archive-command))
  (:lambda (source)
    (destructuring-bind (col1 cols) source
      (list* col1 cols))))

(defrule filename-or-http-uri (or http-uri maybe-quoted-filename))

(defrule archive-source (and kw-load kw-archive kw-from filename-or-http-uri)
  (:lambda (src)
    (destructuring-bind (load from archive source) src
      (declare (ignore load from archive))
      source)))

(defrule load-archive (and archive-source
			   target
			   (? before-load-do)
			   archive-command-list
			   (? finally-do))
  (:lambda (archive)
    (destructuring-bind (source pg-db-uri before commands finally) archive
      (destructuring-bind (&key host port user password dbname &allow-other-keys)
	  pg-db-uri
	`(lambda ()
	   (let* ((state-before  (when ',before
				   (pgloader.utils:make-pgstate)))
		  (*state*        (pgloader.utils:make-pgstate))
		  (state-finally  (when ',finally
				    (pgloader.utils:make-pgstate)))
		  (archive-file
		   ,(destructuring-bind (kind url) source
		     (ecase kind
		       (:http     `(with-stats-collection
				       (,dbname "download" :state state-before)
				     (pgloader.archive:http-fetch-file ,url)))
		       (:filename url))))
		  (*csv-path-root*
		   (with-stats-collection (,dbname "extract" :state state-before)
		     (pgloader.archive:expand-archive archive-file)))
		  (*pgconn-host* ,host)
		  (*pgconn-port* ,port)
		  (*pgconn-user* ,user)
		  (*pgconn-pass* ,password))
	     (progn
	       ;; before block
	       ,(when before
		 `(with-stats-collection (,dbname "before load" :state state-before)
		    (with-pgsql-transaction (,dbname)
		      (loop for command in ',before
			 do
			   (log-message :notice command)
			   (pgsql-execute command :client-min-messages :error)))))

	       ;; import from files block
	       ,@(loop for command in commands
		    collect `(funcall ,command))

	       ;; finally block
	       ,(when finally
		 `(with-stats-collection (,dbname "finally" :state state-finally)
		    (with-pgsql-transaction (,dbname)
		      (loop for command in ',finally
			 do
			   (log-message :notice command)
			   (pgsql-execute command :client-min-messages :error)))))

	       ;; reporting
	       (report-full-summary *state* state-before state-finally
				    "Total import time"))))))))


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
			  load-dbf-file
			  load-database
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
     when (and (listp s-exp) (= 2 (length s-exp)) (eq :inline (first s-exp)))
     collect (list :inline position)
     else collect (if (listp s-exp)
		      (inject-inline-data-position s-exp position)
		      s-exp)))

(defun parse-commands-from-file (filename)
  "The command could be using from :inline, in which case we want to parse
   as much as possible then use the command against an already opened stream
   where we moved at the beginning of the data."
  (let ((*data-expected-inline* nil)
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
	  (parse 'commands content)))))

(defun run-commands (source)
  "SOURCE can be a function, which is run, a list, which is compiled as CL
   code then run, a pathname containing one or more commands that are parsed
   then run, or a commands string that is then parsed and each command run."
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

    ;; Start the logger
    (start-logger)

    ;; run the commands
    (loop for func in funcs do (funcall func))))


;;;
;;; Interactive tool
;;;
(defmacro with-database-uri ((database-uri) &body body)
  "Run the BODY forms with the connection parameters set to proper values
   from the DATABASE-URI. For a MySQL connection string, that's
   *myconn-user* and all, for a PostgreSQL connection string, *pgconn-user*
   and all."
  (destructuring-bind (&key type user password host port &allow-other-keys)
      (parse 'db-connection-uri database-uri)
    (ecase type
      (:mysql
       `(let* ((*myconn-host* ,host)
	       (*myconn-port* ,port)
	       (*myconn-user* ,user)
	       (*myconn-pass* ,password))
	  ,@body))
      (:postgresql
       `(let* ((*pgconn-host* ,host)
	       (*pgconn-port* ,port)
	       (*pgconn-user* ,user)
	       (*pgconn-pass* ,password))
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



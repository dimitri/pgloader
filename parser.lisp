;;;
;;; Parse the pgloader commands grammar
;;;

(in-package :pgloader.parser)

(defparameter *default-host* "localhost")
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
  ;; option for MySQL imports
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
  (def-keyword-rule "identifiers"))

(defrule kw-auto-increment (and "auto_increment" (* (or #\Tab #\Space)))
  (:constant :auto-increment))



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
		    (*pg-settings* ',gucs))
	       (declare (special pgloader.mysql:*cast-rules*
				 *myconn-host* *myconn-port*
				 *myconn-user* *myconn-pass*
				 *pgconn-host* *pgconn-port*
				 *pgconn-user* *pgconn-pass*))
	       (pgloader.mysql:stream-database ,mydb
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

(defrule option-table-name (and kw-table kw-name equal-sign qualified-table-name)
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
    LOAD CSV FROM /Users/dim/dev/CL/pgloader/galaxya/yagoa/communaute_profil.csv
        INTO postgresql://dim@localhost:54393/yagoa?commnaute_profil

        WITH truncate,
             fields optionally enclosed by '\"',
             fields escaped by \"\,
             fields terminated by '\t',
             reset sequences;
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

(defrule option-fields-enclosed-by
    (and kw-fields (? kw-optionally) kw-enclosed kw-by separator)
  (:lambda (enc)
    (destructuring-bind (f e o b sep) enc
      (declare (ignore f e o b))
      (cons :quote sep))))

(defrule option-fields-escaped-by (and kw-fields kw-escaped kw-by separator)
  (:lambda (esc)
    (destructuring-bind (f e b sep) esc
      (declare (ignore f e b))
      (cons :escape sep))))

(defrule option-fields-terminated-by (and kw-fields kw-terminated kw-by separator)
  (:lambda (term)
    (destructuring-bind (fields terminated by sep) term
      (declare (ignore fields terminated by))
      (cons :separator sep))))

(defrule csv-option (or option-truncate
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

(defrule csv-source (and kw-load kw-csv kw-from maybe-quoted-filename)
  (:lambda (src)
    (destructuring-bind (load csv from source) src
      (declare (ignore load csv from))
      ;; source is (:filename #P"pathname/here")
      (destructuring-bind (type uri) source
	(ecase type
	  (:filename uri))))))

(defrule load-csv-file (and csv-source target csv-options)
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
	     (pgloader.csv:copy-from-file ,dbname
					  ,table-name
					  ,source
					  ,@options)))))))


;;;
;;; Now the main command, one of
;;;
;;;  - LOAD FROM some files
;;;  - LOAD DATABASE FROM a MySQL remote database
;;;  - LOAD MESSAGES FROM a syslog daemon receiver we're going to start here
;;;
(defrule end-of-command (and ignore-whitespace #\; ignore-whitespace)
  (:constant :eoc))

(defrule command (and (or load-csv-file
			  load-dbf-file
			  load-database
			  load-syslog-messages)
		      end-of-command)
  (:lambda (cmd)
    (destructuring-bind (command eoc) cmd
      (declare (ignore eoc))
      command)))

(defrule commands (+ command))

(defun parse-command (command)
  "Parse a command and return a LAMBDA form that takes no parameter."
  (parse 'command command))

(defun run-command (command)
  "Parse given COMMAND then run it."
  (let* ((code    (parse-command command))
	 (func    (compile nil code)))
    (funcall func)))


(defun test-parsing ()
  (parse-command "
LOAD FROM http:///tapoueh.org/db.t
     INTO postgresql://localhost:6432/db?t"))

(defun test-parsing-load-database ()
  (parse-command "
    LOAD DATABASE FROM mysql://localhost:3306/dbname
        INTO postgresql://localhost/db
	WITH drop tables,
	     truncate,
	     create tables,
	     create indexes,
	     reset sequences,
             downcase identifiers
	 SET guc_1 = 'value', guc_2 = 'other value'
	CAST column col1 to timestamptz drop default using zero-dates-to-null,
             type varchar to text,
             type int with extra auto_increment to bigserial,
             type datetime to timestamptz drop default using zero-dates-to-null,
             type date drop not null drop default using zero-dates-to-null;
"))

(defun test-parsing-syslog-server ()
  (parse-command "
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
"))

(defun test-parsing-load-from-csv ()
  (parse-command "
    LOAD CSV FROM '/Users/dim/dev/CL/pgloader/galaxya/yagoa/communaute_profil.csv'
        Into postgresql://dim@localhost:54393/yagoa?communaute_profil

        WITH truncate,
             fields optionally enclosed by '\"',
             fields escaped by '\"',
             fields terminated by '\t';
"))

(defun test-parsing-load-from-dbf ()
  (parse-command "
    LOAD DBF FROM '/Users/dim/Downloads/comsimp2013.dbf'
        INTO postgresql://dim@localhost:54393/dim?comsimp2013
        WITH truncate, create table, table name = 'comsimp2013'; "))

(defun test-parsing-lots ()
  (parse 'commands "
    LOAD DATABASE FROM mysql://localhost:3306/dbname
        INTO postgresql://localhost/db
	WITH drop tables,
		 truncate,
		 create tables,
		 create indexes,
		 reset sequences
	 SET guc_1 = 'value', guc_2 = 'other value'
	CAST column col1 to timestamptz drop default using zero-dates-to-null,
             type varchar to text,
             type int with extra auto_increment to bigserial,
             type datetime to timestamptz drop default using zero-dates-to-null,
             type date drop not null drop default using zero-dates-to-null;

    LOAD CSV FROM '/Users/dim/dev/CL/pgloader/galaxya/yagoa/communaute_profil.csv'
        Into postgresql://dim@localhost:54393/yagoa?communaute_profil

        WITH truncate,
             fields optionally enclosed by '\"',
             fields escaped by '\"',
             fields terminated by '\t';

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
"))

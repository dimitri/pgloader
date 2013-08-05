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

;;
;; Keywords
;;
(defmacro def-keyword-rule (keyword)
  (let ((rule-name (read-from-string (format nil "kw-~a" keyword)))
	(constant  (read-from-string (format nil ":~a" keyword))))
    `(defrule ,rule-name (and ignore-whitespace (~ ,keyword) ignore-whitespace)
       (:constant ',constant))))

(eval-when (:load-toplevel :compile-toplevel :execute)
  (def-keyword-rule "load")
  (def-keyword-rule "data")
  (def-keyword-rule "from")
  (def-keyword-rule "into")
  (def-keyword-rule "with")
  (def-keyword-rule "set")
  (def-keyword-rule "database")
  (def-keyword-rule "cast")
  (def-keyword-rule "column")
  (def-keyword-rule "type")
  (def-keyword-rule "extra")
  (def-keyword-rule "drop")
  (def-keyword-rule "not")
  (def-keyword-rule "to")
  (def-keyword-rule "null")
  (def-keyword-rule "default")
  (def-keyword-rule "using"))

(defrule kw-auto-increment (and "auto_increment" (* (or #\Tab #\Space)))
  (:constant :auto-increment))

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

;;
;; The main target parsing
;;
;;  COPY postgresql://user@localhost:5432/dbname?foo
;;
(defrule target (and kw-into db-connection-uri)
  (:destructure (into target)
    (declare (ignore into))
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
	 (pgloader.pgsql:copy-from-file ,dbname ,table-name ',source)))))

(defrule database-source (and ignore-whitespace
			      kw-load kw-database kw-from
			      db-connection-uri)
  (:lambda (source)
    (destructuring-bind (nil l d f uri) source
      (declare (ignore l d f))
      uri)))

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

(defrule option (and optname (? equal-sign) (? optvalue))
  (:lambda (source)
    (destructuring-bind (name es value) source
      (declare (ignore es))
      (cons name value))))

(defrule another-option (and #\, ignore-whitespace option)
  (:lambda (source)
    (destructuring-bind (comma ws option) source
      (declare (ignore comma ws))
      option)))

(defrule option-list (and option (* another-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (list* opt1 opts))))

(defrule end-of-option-list (and ignore-whitespace
				 #\;
				 ignore-whitespace)
  (:constant :eol))

(defrule options (and kw-with option-list end-of-option-list)
  (:lambda (source)
    (destructuring-bind (w opts eol) source
      (declare (ignore w eol))
      opts)))

(defrule gucs (and kw-set option-list end-of-option-list)
  (:lambda (source)
    (destructuring-bind (set gucs eol) source
      (declare (ignore set eol))
      gucs)))

;;
;; Now parsing CAST rules
;;

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

(defrule cast-rule (and cast-source cast-def (? cast-function))
  (:lambda (cast)
    (destructuring-bind (source target function) cast
      (list :source source :target target :using function))))

(defrule another-cast-rule (and #\, ignore-whitespace cast-rule)
  (:lambda (source)
    (destructuring-bind (comma ws rule) source
      (declare (ignore comma ws))
      rule)))

(defrule cast-rule-list (and cast-rule (* another-cast-rule))
  (:lambda (source)
    (destructuring-bind (rule1 rules) source
      (list* rule1 rules))))

(defrule casts (and kw-cast cast-rule-list end-of-option-list)
  (:lambda (source)
    (destructuring-bind (c casts eol) source
      (declare (ignore c eol))
      casts)))

(defrule load-database (and database-source target
			    (? options)
			    (? gucs)
			    (? casts))
  (:lambda (source)
    (destructuring-bind (my-db-uri pg-db-uri options gucs casts) source
      (list :myconn my-db-uri
	    :pgconn pg-db-uri
	    :opts options
	    :gucs gucs
	    :casts casts))))

(defun parse-load (string)
  (parse 'load string))

(defun test-parsing ()
  (parse-load "
LOAD FROM http:///tapoueh.org/db.t
     INTO postgresql://localhost:6432/db?t"))

(defun test-parsing-load-database ()
  (parse 'load-database "
    LOAD DATABASE FROM mysql://localhost:3306/dbname
        INTO postgresql://localhost/db
	WITH drop tables,
		 create tables,
		 create indexes,
		 reset sequences;
	 SET guc_1 = 'value', guc_2 = 'other value';
	CAST column col1 to timestamptz drop default using zero-dates-to-null,
             type varchar to text,
             type int with extra auto_increment to bigserial,
             type datetime to timestamptz drop default using zero-dates-to-null,
             type date drop not null drop default using zero-dates-to-null;
"))


(defun test-loading-code ()
  "Have a try at writing the code we want the parser to generate."
  (let* ((*cast-rules*
	  '((:source (:column "col1" :auto-increment nil)
	     :target (:type "timestamptz" :drop-default t :drop-not-null nil)
	     :using pgloader.transforms::zero-dates-to-null)
	    (:source (:type "varchar" :auto-increment nil)
	     :target (:type "text" :drop-default nil :drop-not-null nil)
	     :using nil)
	    (:source (:type "int" :auto-increment t)
	     :target (:type "bigserial" :drop-default nil :drop-not-null nil)
	     :using nil)
	    (:source (:type "datetime" :auto-increment nil)
	     :target (:type "timestamptz" :drop-default t :drop-not-null nil)
	     :using pgloader.transforms::zero-dates-to-null)
	    (:source (:type "date" :auto-increment nil)
	     :target (:type nil :drop-default t :drop-not-null t)
	     :using pgloader.transforms::zero-dates-to-null)))
	 ;; MySQL Connection Parameters
	 ;; TODO: port
	 (*myconn-host* "localhost")
	 (*myconn-user* nil)
	 (*myconn-pass* nil)
	 ;; PostgreSQL Connection Parameters
	 ;; TODO: PostgreSQL GUCs
	 (*pgconn-host* "localhost")
	 (*pgconn-port* 5432)
	 (*pgconn-user* nil)
	 (*pgconn-pass* nil))
    ;; TODO: arrange parsed options
    ;; TODO: use given transform functions
    (pgloader.mysql:stream-database "dbname"
				    :pg-dbname "dbname"
				    :create-tables t
				    :include-drop t
				    :truncate nil
				    :reset-sequences t)))

;;;
;;; Tools to handle PostgreSQL queries
;;;
(in-package :pgloader.pgsql)

;;;
;;; PostgreSQL Tools connecting to a database
;;;
(defclass pgsql-connection (db-connection)
  ((use-ssl :initarg :use-ssl :accessor pgconn-use-ssl)
   (table-name :initarg :table-name :accessor pgconn-table-name))
  (:documentation "PostgreSQL connection for pgloader"))

(defmethod initialize-instance :after ((pgconn pgsql-connection) &key)
  "Assign the type slot to pgsql."
  (setf (slot-value pgconn 'type) "pgsql"))

(defmethod clone-connection ((c pgsql-connection))
  (let ((clone
         (change-class (call-next-method c) 'pgsql-connection)))
    (setf (pgconn-use-ssl clone)    (pgconn-use-ssl c)
          (pgconn-table-name clone) (pgconn-table-name c))
    clone))

(defmethod ssl-enable-p ((pgconn pgsql-connection))
  "Return non-nil when the connection uses SSL"
  (member (pgconn-use-ssl pgconn) '(:try :yes)))

(defun new-pgsql-connection (pgconn)
  "Prepare a new connection object with all the same properties as pgconn,
   so as to avoid stepping on it's handle"
  (make-instance 'pgsql-connection
                 :user (db-user pgconn)
                 :pass (db-pass pgconn)
                 :host (db-host pgconn)
                 :port (db-port pgconn)
                 :name (db-name pgconn)
                 :use-ssl (pgconn-use-ssl pgconn)
                 :table-name (pgconn-table-name pgconn)))

;;;
;;; Implement SSL Client Side certificates
;;; http://www.postgresql.org/docs/current/static/libpq-ssl.html#LIBPQ-SSL-FILE-USAGE
;;;
(defvar *pgsql-client-certificate* "~/.postgresql/postgresql.crt"
  "File where to read the PostgreSQL Client Side SSL Certificate.")

(defvar *pgsql-client-key* "~/.postgresql/postgresql.key"
  "File where to read the PostgreSQL Client Side SSL Private Key.")

;;;
;;; PostgreSQL errors types  for pgloader.
;;;
(deftype postgresql-retryable ()
  "PostgreSQL errors that we know how to retry in a batch."
  `(or
    cl-postgres-error::data-exception
    cl-postgres-error::integrity-violation
    cl-postgres-error:internal-error
    cl-postgres-error::insufficient-resources
    cl-postgres-error::program-limit-exceeded))

(deftype postgresql-unavailable ()
    "It might happen that PostgreSQL becomes unavailable in the middle of
     our processing: it being restarted is an example."
  `(or
    cl-postgres-error::server-shutdown
    cl-postgres-error::admin-shutdown
    cl-postgres-error::crash-shutdown
    cl-postgres-error::operator-intervention
    cl-postgres-error::cannot-connect-now
    cl-postgres-error::database-connection-error
    cl-postgres-error::database-connection-lost
    cl-postgres-error::database-socket-error))

;;;
;;; We need to distinguish some special cases of PostgreSQL errors within
;;; Class 53 — Insufficient Resources: in case of "too many connections" we
;;; typically want to leave room for another worker to finish and free one
;;; connection, then try again.
;;;
;;; http://www.postgresql.org/docs/9.4/interactive/errcodes-appendix.html
;;;
;;; The "leave room to finish and try again" heuristic is currently quite
;;; simplistic, but at least it work in my test cases.
;;;
(cl-postgres-error::deferror "53300"
    too-many-connections cl-postgres-error:insufficient-resources)
(cl-postgres-error::deferror "53400"
    configuration-limit-exceeded cl-postgres-error:insufficient-resources)

(defvar *retry-connect-times* 5
  "How many times to we try to connect again.")

(defvar *retry-connect-delay* 0.5
  "How many seconds to wait before trying to connect again.")

(defmethod open-connection ((pgconn pgsql-connection) &key username)
  "Open a PostgreSQL connection."
  (let* (#+unix
         (cl-postgres::*unix-socket-dir*  (get-unix-socket-dir pgconn))
         (crt-file (expand-user-homedir-pathname *pgsql-client-certificate*))
         (key-file (expand-user-homedir-pathname *pgsql-client-key*))
         (pomo::*ssl-certificate-file* (when (and (ssl-enable-p pgconn)
                                                  (probe-file crt-file))
                                         (uiop:native-namestring crt-file)))
         (pomo::*ssl-key-file*         (when (and (ssl-enable-p pgconn)
                                                  (probe-file key-file))
                                         (uiop:native-namestring key-file))))
    (flet ((connect (pgconn username)
             (handler-case
                 ;; in some cases (client_min_messages set to debug5
                 ;; for example), PostgreSQL might send us some
                 ;; WARNINGs already when opening a new connection
                 (handler-bind ((cl-postgres:postgresql-warning
                                 #'(lambda (w)
                                     (log-message :warning "~a" w)
                                     (muffle-warning))))
                   (pomo:connect (db-name pgconn)
                                 (or username (db-user pgconn))
                                 (db-pass pgconn)
                                 (let ((host (db-host pgconn)))
                                   (if (and (consp host) (eq :unix (car host)))
                                       :unix
                                       host))
                                 :port (db-port pgconn)
                                 :use-ssl (or (pgconn-use-ssl pgconn) :no)))
               ((or too-many-connections configuration-limit-exceeded) (e)
                 (log-message :error
                              "Failed to connect to ~a: ~a; will try again in ~fs"
                              pgconn e *retry-connect-delay*)
                 (sleep *retry-connect-delay*)))))
      (loop :while (null (conn-handle pgconn))
         :repeat *retry-connect-times*
         :do (setf (conn-handle pgconn) (connect pgconn username))))

    (unless (conn-handle pgconn)
      (error "Failed ~d times to connect to ~a" *retry-connect-times* pgconn))

    (log-message :debug "CONNECTED TO ~s" pgconn)
    (set-session-gucs *pg-settings* :database (conn-handle pgconn))

    pgconn))

(defmethod close-connection ((pgconn pgsql-connection))
  "Close a PostgreSQL connection."
  (assert (not (null (conn-handle pgconn))))
  (pomo:disconnect (conn-handle pgconn))
  (setf (conn-handle pgconn) nil)
  pgconn)

(defmethod query ((pgconn (eql nil)) sql &key)
  "Case when a connection already exists around the call, as per
   `with-connection' and `with-transaction'."
  (log-message :sql "~a" sql)
  (pomo:query sql))

(defmethod query ((pgconn pgsql-connection) sql &key)
  (let ((pomo:*database* (conn-handle pgconn)))
    (log-message :sql "~a" sql)
    (pomo:query sql)))

(defmacro handling-pgsql-notices (&body forms)
  "The BODY is run within a PostgreSQL transaction where *pg-settings* have
   been applied. PostgreSQL warnings and errors are logged at the
   appropriate log level."
  `(handler-bind
       (((and cl-postgres:database-error
              (not postgresql-unavailable))
          #'(lambda (e)
              (log-message :error "~a" e)))
	(cl-postgres:postgresql-warning
	 #'(lambda (w)
	     (log-message :warning "~a" w)
	     (muffle-warning))))
     (progn ,@forms)))

(defmacro with-pgsql-transaction ((&key pgconn database) &body forms)
  "Run FORMS within a PostgreSQL transaction to DBNAME, reusing DATABASE if
   given."
  (if database
      `(let ((pomo:*database* ,database))
	 (handling-pgsql-notices
              (pomo:with-transaction ()
                (log-message :debug "BEGIN")
                ,@forms)))
      ;; no database given, create a new database connection
      `(with-pgsql-connection (,pgconn)
         (pomo:with-transaction ()
           (log-message :debug "BEGIN")
           ,@forms))))

(defmacro with-pgsql-connection ((pgconn) &body forms)
  "Run FROMS within a PostgreSQL connection to DBNAME. To get the connection
   spec from the DBNAME, use `get-connection-spec'."
  (let ((conn (gensym "pgsql-conn")))
    `(with-connection (,conn ,pgconn)
       (let ((pomo:*database* (conn-handle ,conn)))
         (handling-pgsql-notices
           ,@forms)))))

(defun get-unix-socket-dir (pgconn)
  "When *pgconn* host is a (cons :unix path) value, return the right value
   for cl-postgres::*unix-socket-dir*."
  (let ((host (db-host pgconn)))
    (if (and (consp host) (eq :unix (car host)))
        ;; set to *pgconn* host value
        (directory-namestring (fad:pathname-as-directory (cdr host)))
        ;; keep as is.
        cl-postgres::*unix-socket-dir*)))

(defun set-session-gucs (alist &key transaction database)
  "Set given GUCs to given values for the current session."
  (let ((pomo:*database* (or database pomo:*database*)))
    (loop
       :for (name . value) :in alist
       :for set := (cond
                     ((string-equal "search_path" name)
                      ;; for search_path, don't quote the value
                      (format nil "SET~:[~; LOCAL~] ~a TO ~a"
                              transaction name value))
                     (t
                      ;; general case: quote the value
                      (format nil "SET~:[~; LOCAL~] ~a TO '~a'"
                              transaction name value)))
       :do (progn                       ; indent helper
             (log-message :debug set)
             (pomo:execute set)))))



;;;
;;; The parser is still hard-coded to support only PostgreSQL targets
;;;
(defun sanitize-user-gucs (gucs)
  "Forbid certain actions such as setting a client_encoding different from utf8."
  (let ((gucs
         (append
          (list (cons "client_encoding" "utf8"))
          (loop :for (name . value) :in gucs
             :when    (and (string-equal name "client_encoding")
                           (not (member value '("utf-8" "utf8") :test #'string-equal)))
             :do      (log-message :warning
                                   "pgloader always talk to PostgreSQL in utf-8, client_encoding has been forced to 'utf8'.")
             :else
             :collect (cons name value)))))
    ;;
    ;; Now see about the application_name, provide "pgloader" if it's not
    ;; been overloaded already.
    ;;
    (cond ((not (assoc "application_name" gucs :test #'string-equal))
           (append gucs (list (cons "application_name" "pgloader"))))

          (t
           gucs))))


;;;
;;; DDL support with stats (timing, object count)
;;;
(defun pgsql-connect-and-execute-with-timing (pgconn section label sql)
  "Run pgsql-execute-with-timing within a newly establised connection."
  (handler-case
      (with-pgsql-connection (pgconn)
        (pomo:with-transaction ()
          (pgsql-execute-with-timing section label sql :log-level :notice)))

    (postgresql-unavailable (condition)

      (log-message :error "~a" condition)
      (log-message :error "Reconnecting to PostgreSQL")

     ;; in order to avoid Socket error in "connect": ECONNREFUSED if we
     ;; try just too soon, wait a little
      (sleep 2)

      (pgsql-connect-and-execute-with-timing pgconn section label sql))))

(defun pgsql-execute-with-timing (section label sql-list
                                  &key
                                    (log-level :sql)
                                    on-error-stop
                                    client-min-messages)
  "Execute given SQL and resgister its timing into STATE."
  (let ((sql-list (alexandria:ensure-list sql-list)))
    (multiple-value-bind (res secs)
        (timing
          (multiple-value-bind (nb-ok nb-errors)
              (pgsql-execute sql-list
                             :log-level log-level
                             :on-error-stop on-error-stop
                             :client-min-messages client-min-messages)
            (update-stats section label :rows nb-ok :errs nb-errors)))
      (declare (ignore res))
      (update-stats section label :read (length sql-list) :secs secs))))

(defun pgsql-execute (sql
                      &key
                        (log-level :sql)
                        client-min-messages
                        (on-error-stop t))
  "Execute given SQL list of statements in current transaction.

   When ON-ERROR-STOP is non-nil (the default), we stop at the first sql
   statement that fails. That's because this facility is meant for DDL. With
   ON_ERROR_STOP nil, log the problem and continue thanks to PostgreSQL
   savepoints."
  (let ((sql-list  (alexandria::ensure-list sql))
        (nb-ok     0)
        (nb-errors 0))
    (when client-min-messages
      (pomo:execute
       (format nil "SET LOCAL client_min_messages TO ~a;"
               (symbol-name client-min-messages))))

    (if on-error-stop
        (loop :for sql :in sql-list
           :do (progn
                 (log-message log-level "~a" sql)
                 (pomo:execute sql))
           ;; never executed in case of error, which signals out of here
           :finally (incf nb-ok (length sql-list)))

        ;; handle failures and just continue
        (loop :for sql :in sql-list
           :do (progn
                 (pomo:execute "savepoint pgloader;")
                 (handler-case
                     (progn
                       (log-message log-level "~a" sql)
                       (pomo:execute sql)
                       (pomo:execute "release savepoint pgloader;")
                       (incf nb-ok))
                   (cl-postgres:database-error (e)
                     (incf nb-errors)
                     (log-message :error "PostgreSQL ~a" e)
                     (pomo:execute "rollback to savepoint pgloader;"))))))

    (when client-min-messages
      (pomo:execute (format nil "RESET client_min_messages;")))

    (values nb-ok nb-errors)))


;;;
;;; PostgreSQL version specific support, that we get once connected
;;;
(defun list-typenames-without-btree-support ()
  "Fetch PostgresQL data types without btree support, so that it's possible
   to later CREATE INDEX ... ON ... USING gist(...), or even something else
   than gist. "
  (loop :for (typename access-methods) :in
     (pomo:query (sql "/pgsql/list-typenames-without-btree-support.sql"))
     :collect (cons typename access-methods)))

(defun list-reserved-keywords (pgconn)
  "Connect to PostgreSQL DBNAME and fetch reserved keywords."
  (handler-case
      (with-pgsql-connection (pgconn)
        (pomo:query "select word
                   from pg_get_keywords()
                  where catcode IN ('R', 'T')" :column))
    ;; support for Amazon Redshift
    (cl-postgres-error::syntax-error-or-access-violation (e)
      ;; 42883	undefined_function
      ;;    Database error 42883: function pg_get_keywords() does not exist
      ;;
      ;; the following list comes from a manual query against a local
      ;; PostgreSQL server (version 9.5devel), it's better to have this list
      ;; than nothing at all.
      (declare (ignore e))
      (list "all"
            "analyse"
            "analyze"
            "and"
            "any"
            "array"
            "as"
            "asc"
            "asymmetric"
            "authorization"
            "binary"
            "both"
            "case"
            "cast"
            "check"
            "collate"
            "collation"
            "column"
            "concurrently"
            "constraint"
            "create"
            "cross"
            "current_catalog"
            "current_date"
            "current_role"
            "current_schema"
            "current_time"
            "current_timestamp"
            "current_user"
            "default"
            "deferrable"
            "desc"
            "distinct"
            "do"
            "else"
            "end"
            "except"
            "false"
            "fetch"
            "for"
            "foreign"
            "freeze"
            "from"
            "full"
            "grant"
            "group"
            "having"
            "ilike"
            "in"
            "initially"
            "inner"
            "intersect"
            "into"
            "is"
            "isnull"
            "join"
            "lateral"
            "leading"
            "left"
            "like"
            "limit"
            "localtime"
            "localtimestamp"
            "natural"
            "not"
            "notnull"
            "null"
            "offset"
            "on"
            "only"
            "or"
            "order"
            "outer"
            "overlaps"
            "placing"
            "primary"
            "references"
            "returning"
            "right"
            "select"
            "session_user"
            "similar"
            "some"
            "symmetric"
            "table"
            "then"
            "to"
            "trailing"
            "true"
            "union"
            "unique"
            "user"
            "using"
            "variadic"
            "verbose"
            "when"
            "where"
            "window"
            "with"))))

;;;
;;; The main API, or an attempt at providing pgloader as a lisp usable API
;;; rather than only an end-user program.
;;;

(in-package #:pgloader)

(define-condition source-definition-error (error)
  ((mesg :initarg :mesg :reader source-definition-error-mesg))
  (:report (lambda (err stream)
             (format stream "~a" (source-definition-error-mesg err)))))

(define-condition cli-parsing-error (error) ()
  (:report (lambda (err stream)
             (declare (ignore err))
             (format stream "Could not parse the command line: see above."))))

(define-condition load-files-not-found-error (error)
  ((filename-list :initarg :filename-list))
  (:report (lambda (err stream)
             (format stream
                     ;; start lines with 3 spaces because of trivial-backtrace
                     "~{No such file or directory: ~s~^~%   ~}"
                     (slot-value err 'filename-list)))))

;;;
;;; Helper functions to actually do things
;;;
(defun process-command-file (filename-list &key (flush-summary t))
  "Process each FILENAME in FILENAME-LIST as a pgloader command
   file (.load)."
  (loop :for filename :in filename-list
     :for truename := (probe-file filename)
     :unless truename :collect filename :into not-found-list
     :do (if truename
             (run-commands truename
                           :start-logger nil
                           :flush-summary flush-summary)
             (log-message :error "Can not find file: ~s" filename))
     :finally (when not-found-list
                (error 'load-files-not-found-error :filename-list not-found-list))))

(defun process-source-and-target (source-string target-string
                                  &optional
                                    type encoding set with field cast
                                    before after)
  "Given exactly 2 CLI arguments, process them as source and target URIs.
Parameters here are meant to be already parsed, see parse-cli-optargs."
  (let* ((type       (handler-case
                         (parse-cli-type type)
                       (condition (e)
                         (log-message :warning
                                      "Could not parse --type ~s: ~a"
                                      type e))))
         (source-uri (handler-case
                         (if type
                             (parse-source-string-for-type type source-string)
                             (parse-source-string source-string))
                       (condition (e)
                         (log-message :warning
                                      "Could not parse source string ~s: ~a"
                                      source-string e))))
         (type       (when (and source-string
                                (typep source-uri 'connection))
                       (parse-cli-type (conn-type source-uri))))
         (target-uri (handler-case
                         (parse-target-string target-string)
                       (condition (e)
                         (log-message :error
                                      "Could not parse target string ~s: ~a"
                                      target-string e)))))

    ;; some verbosity about the parsing "magic"
    (log-message :info "    SOURCE: ~s" source-string)
    (log-message :info "SOURCE URI: ~s" source-uri)
    (log-message :info "    TARGET: ~s" target-string)
    (log-message :info "TARGET URI: ~s" target-uri)

    (cond ((and (null source-uri) (null target-uri))
           (process-command-file (list source-string target-string)))

          ((or (null source-string) (null source-uri))
           (log-message :fatal
                        "Failed to parse ~s as a source URI." source-string)
           (log-message :log "You might need to use --type."))

          ((or (null target-string) (null target-uri))
           (log-message :fatal
                        "Failed to parse ~s as a PostgreSQL database URI."
                        target-string)))

    (let* ((nb-errors 0)
           (options (handler-case
                        (parse-cli-options type with)
                      (condition (e)
                        (incf nb-errors)
                        (log-message :error "Could not parse --with ~s:" with)
                        (log-message :error "~a" e))))
           (fields  (handler-case
                        (parse-cli-fields type field)
                      (condition (e)
                        (incf nb-errors)
                        (log-message :error "Could not parse --fields ~s:" field)
                        (log-message :error "~a" e)))))

      (destructuring-bind (&key encoding gucs casts before after)
          (loop :for (keyword option user-string parse-fn)
             :in `((:encoding  "--encoding" ,encoding ,#'parse-cli-encoding)
                   (:gucs      "--set"      ,set      ,#'parse-cli-gucs)
                   (:casts     "--cast"     ,cast     ,#'parse-cli-casts)
                   (:before    "--before"   ,before   ,#'parse-sql-file)
                   (:after     "--after"    ,after    ,#'parse-sql-file))
             :append (list keyword
                           (handler-case
                               (funcall parse-fn user-string)
                             (condition (e)
                               (incf nb-errors)
                               (log-message :error "Could not parse ~a ~s: ~a"
                                            option user-string e)))))

        (unless (= 0 nb-errors)
          (error 'cli-parsing-error))

        ;; so, we actually have all the specs for the
        ;; job on the command line now.
        (when (and source-uri target-uri (= 0 nb-errors))
          (load-data :from source-uri
                     :into target-uri
                     :encoding encoding
                     :options  options
                     :gucs     gucs
                     :fields   fields
                     :casts    casts
                     :before   before
                     :after    after
                     :start-logger nil))))))


;;;
;;; Helper function to run a given command
;;;
(defun run-commands (source
		     &key
		       (start-logger t)
                       (flush-summary t)
                       ((:summary *summary-pathname*) *summary-pathname*)
		       ((:log-filename *log-filename*) *log-filename*)
		       ((:log-min-messages *log-min-messages*) *log-min-messages*)
		       ((:client-min-messages *client-min-messages*) *client-min-messages*))
  "SOURCE can be a function, which is run, a list, which is compiled as CL
   code then run, a pathname containing one or more commands that are parsed
   then run, or a commands string that is then parsed and each command run."

  (with-monitor (:start-logger start-logger)
    (let* ((*print-circle* nil)
           (funcs
            (typecase source
              (function (list source))

              (list     (list (compile-lisp-command source)))

              (pathname (mapcar #'compile-lisp-command
                                (parse-commands-from-file source)))

              (t        (mapcar #'compile-lisp-command
                                (if (probe-file source)
                                    (parse-commands-from-file source)
                                    (parse-commands source)))))))

      (loop :for func :in funcs
         :do (funcall func)
         :do (when flush-summary
               (flush-summary :reset t))))))

(defun compile-lisp-command (source)
  "SOURCE must be lisp source code, a list form."
  (let (function warnings-p failure-p notes)
    ;; capture the compiler notes and warnings
    (setf notes
          (with-output-to-string (stream)
            (let ((*standard-output* stream)
                  (*error-output* stream)
                  (*trace-output* stream))
              (with-compilation-unit (:override t)
                (setf (values function warnings-p failure-p)
                      (compile nil source))))))

    ;; log the captured compiler output at the DEBUG level
    (when (and notes (string/= notes ""))
      (let ((pp-source (with-output-to-string (s) (pprint source s))))
        (log-message :debug "While compiling:~%~a~%~a" pp-source notes)))

    ;; and signal an error if we failed to compile our lisp code
    (cond
      (failure-p   (error "Failed to compile code: ~a~%~a" source notes))
      (warnings-p  function)
      (t           function))))


;;;
;;; Main API to use from outside of pgloader.
;;;
(defun load-data (&key ((:from source)) ((:into target))
                    encoding fields target-table-name
                    options gucs casts before after
                    (start-logger t) (flush-summary t))
  "Load data from SOURCE into TARGET."
  (declare (type connection source)
           (type pgsql-connection target))

  (when (and (typep source (or 'csv-connection
                               'copy-connection
                               'fixed-connection))
             (null target-table-name)
             (null (pgconn-table-name target)))
    (error 'source-definition-error
           :mesg (format nil
                         "~a data source require a table name target."
                         (conn-type source))))

  (with-monitor (:start-logger start-logger)
    (when (and casts (not (member (type-of source)
                                  '(sqlite-connection
                                    mysql-connection
                                    mssql-connection))))
      (log-message :log "Cast rules are ignored for this sources."))

    ;; now generates the code for the command
    (log-message :debug "LOAD DATA FROM ~s" source)
    (let* ((target-table-name (or target-table-name
                                  (pgconn-table-name target)))
           (code (lisp-code-for-loading :from source
                                        :into target
                                        :encoding encoding
                                        :fields fields
                                        :target-table-name target-table-name
                                        :options options
                                        :gucs gucs
                                        :casts casts
                                        :before before
                                        :after after)))
      (run-commands (process-relative-pathnames (uiop:getcwd) code)
                    :start-logger nil
                    :flush-summary flush-summary))))

(defvar *get-code-for-source*
  (list (cons 'copy-connection    #'lisp-code-for-loading-from-copy)
        (cons 'fixed-connection   #'lisp-code-for-loading-from-fixed)
        (cons 'csv-connection     #'lisp-code-for-loading-from-csv)
        (cons 'dbf-connection     #'lisp-code-for-loading-from-dbf)
        (cons 'ixf-connection     #'lisp-code-for-loading-from-ixf)
        (cons 'sqlite-connection  #'lisp-code-for-loading-from-sqlite)
        (cons 'mysql-connection   #'lisp-code-for-loading-from-mysql)
        (cons 'mssql-connection   #'lisp-code-for-loading-from-mssql)
        (cons 'pgsql-connection   #'lisp-code-for-loading-from-pgsql))
  "Each source type might require a different set of options.")

(defun lisp-code-for-loading (&key
                                ((:from source)) ((:into target))
                                encoding fields target-table-name
                                options gucs casts before after)
  (let ((func (cdr (assoc (type-of source) *get-code-for-source*))))
    ;; not all functions support the same set of &key parameters,
    ;; they all have &allow-other-keys in their signature tho.
    (assert (not (null func)))
    (if func
        (funcall func
                 source
                 target
                 :target-table-name target-table-name
                 :fields fields
                 :encoding (or encoding :default)
                 :gucs gucs
                 :casts casts
                 :options options
                 :before before
                 :after after
                 :allow-other-keys t))))

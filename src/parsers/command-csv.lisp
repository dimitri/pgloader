;;;
;;; The main CSV loading command, with optional clauses
;;;

(in-package #:pgloader.parser)

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
    (bind (((_ digits) hex))
      (code-char (parse-integer (text digits) :radix 16)))))

(defrule tab-separator          (and #\' #\\ #\t #\') (:constant #\Tab))
(defrule backslash-separator    (and #\' #\\ #\')     (:constant #\\))

(defrule single-quote-separator (or (and #\' #\' #\' #\')
                                    (and #\' #\\ #\' #\'))
  (:constant #\'))

(defrule other-char-separator (and #\' (or hex-char-code character) #\')
  (:lambda (sep)
    (bind (((_ char _) sep)) char)))

(defrule separator (or single-quote-separator
                       backslash-separator
                       tab-separator
                       other-char-separator))

;;
;; Main CSV options (WITH ... in the command grammar)
;;
(defrule option-skip-header (and kw-skip kw-header equal-sign
				 (+ (digit-char-p character)))
  (:lambda (osh)
    (bind (((_ _ _ digits) osh))
      (cons :skip-lines (parse-integer (text digits))))))

(defrule option-csv-header (and kw-csv kw-header)
  (:constant (cons :header t)))

(defrule option-fields-enclosed-by
    (and kw-fields (? kw-optionally) kw-enclosed kw-by separator)
  (:lambda (enc)
    (bind (((_ _ _ _ sep) enc))
      (cons :quote sep))))

(defrule option-fields-not-enclosed (and kw-fields kw-not kw-enclosed)
  (:constant (cons :quote nil)))

(defrule quote-quote     "double-quote"    (:constant #(#\" #\")))
(defrule backslash-quote "backslash-quote" (:constant #(#\\ #\")))
(defrule escaped-quote-name    (or quote-quote backslash-quote))
(defrule escaped-quote-literal (or (and #\" #\") (and #\\ #\")) (:text t))
(defrule escaped-quote         (or escaped-quote-literal
                                   escaped-quote-name
                                   separator))

(defrule escape-mode-quote     "quote"     (:constant :quote))
(defrule escape-mode-following "following" (:constant :following))
(defrule escape-mode           (or escape-mode-quote escape-mode-following))

(defrule option-fields-escaped-by (and kw-fields kw-escaped kw-by escaped-quote)
  (:lambda (esc)
    (bind (((_ _ _ sep) esc))
      (cons :escape sep))))

(defrule option-terminated-by (and kw-terminated kw-by separator)
  (:lambda (term)
    (bind (((_ _ sep) term))
      (cons :separator sep))))

(defrule option-fields-terminated-by (and kw-fields option-terminated-by)
  (:lambda (term)
    (bind (((_ sep) term)) sep)))

(defrule option-lines-terminated-by (and kw-lines kw-terminated kw-by separator)
  (:lambda (term)
    (bind (((_ _ _ sep) term))
      (cons :newline sep))))

(defrule option-keep-unquoted-blanks (and kw-keep kw-unquoted kw-blanks)
  (:constant (cons :trim-blanks nil)))

(defrule option-trim-unquoted-blanks (and kw-trim kw-unquoted kw-blanks)
  (:constant (cons :trim-blanks t)))

(defrule option-csv-escape-mode (and kw-csv kw-escape kw-mode escape-mode)
  (:lambda (term)
    (bind (((_ _ _ escape-mode) term))
      (cons :escape-mode escape-mode))))

(defrule csv-option (or option-on-error-stop
                        option-on-error-resume-next
                        option-workers
                        option-concurrency
                        option-batch-rows
                        option-batch-size
                        option-prefetch-rows
                        option-max-parallel-create-index
                        option-truncate
                        option-disable-triggers
                        option-identifiers-case
                        option-drop-indexes
                        option-skip-header
                        option-csv-header
                        option-lines-terminated-by
                        option-fields-not-enclosed
                        option-fields-enclosed-by
                        option-fields-escaped-by
                        option-fields-terminated-by
                        option-trim-unquoted-blanks
                        option-keep-unquoted-blanks
                        option-csv-escape-mode
                        option-null-if))

(defrule csv-options (and kw-with
                             (and csv-option (* (and comma csv-option))))
  (:function flatten-option-list))

;;
;; CSV per-field reading options
;;
(defrule single-quoted-string (and #\' (* (not #\')) #\')
  (:lambda (qs)
    (bind (((_ string _) qs))
      (text string))))

(defrule double-quoted-string (and #\" (* (not #\")) #\")
  (:lambda (qs)
    (bind (((_ string _) qs))
      (text string))))

(defrule quoted-string (or single-quoted-string double-quoted-string))

(defrule option-date-format (and kw-date kw-format quoted-string)
  (:lambda (df)
    (bind (((_ _ date-format) df))
      (cons :date-format date-format))))

(defrule blanks kw-blanks (:constant :blanks))

(defrule option-null-if (and kw-null kw-if (or blanks quoted-string))
  (:lambda (nullif)
    (bind (((_ _ opt) nullif))
      (cons :null-as opt))))

(defrule option-trim-both-whitespace (and kw-trim kw-both kw-whitespace)
  (:constant (cons :trim-both t)))

(defrule option-trim-left-whitespace (and kw-trim kw-left kw-whitespace)
  (:constant (cons :trim-left t)))

(defrule option-trim-right-whitespace (and kw-trim kw-right kw-whitespace)
  (:constant (cons :trim-right t)))

(defrule csv-field-option (or option-terminated-by
			      option-date-format
			      option-null-if
                              option-trim-both-whitespace
                              option-trim-left-whitespace
                              option-trim-right-whitespace))

(defrule another-csv-field-option (and comma csv-field-option)
  (:lambda (field-option)
    (bind (((_ option) field-option)) option)))

(defrule open-square-bracket (and ignore-whitespace #\[ ignore-whitespace)
  (:constant :open-square-bracket))
(defrule close-square-bracket (and ignore-whitespace #\] ignore-whitespace)
  (:constant :close-square-bracket))

(defrule csv-field-option-list (and open-square-bracket
                                    csv-field-option
                                    (* another-csv-field-option)
                                    close-square-bracket)
  (:lambda (option)
    (bind (((_ opt1 opts _) option))
      (alexandria:alist-plist `(,opt1 ,@opts)))))

(defrule csv-field-options (? csv-field-option-list))

(defrule csv-bare-field-name (and (or #\_ (alpha-char-p character))
                                  (* (or (alpha-char-p character)
                                         (digit-char-p character)
                                         #\.
                                         #\$
                                         #\_)))
  (:lambda (name)
    (string-downcase (text name))))

(defrule csv-quoted-field-name (or (and #\' (* (not #\')) #\')
                                   (and #\" (* (not #\")) #\"))
  (:lambda (csv-field-name)
    (bind (((_ name _) csv-field-name)) (text name))))

(defrule csv-field-name (or csv-quoted-field-name csv-bare-field-name))

(defrule csv-source-field (and csv-field-name csv-field-options)
  (:destructure (name opts)
    `(,name ,@opts)))

(defrule another-csv-source-field (and comma csv-source-field)
  (:lambda (source)
    (bind (((_ field) source)) field)))

(defrule csv-source-fields (and csv-source-field (* another-csv-source-field))
  (:lambda (source)
    (destructuring-bind (field1 fields) source
      (list* field1 fields))))

(defrule having-fields (and kw-having kw-fields) (:constant nil))

(defrule csv-source-field-list (and (? having-fields)
                                    open-paren csv-source-fields close-paren)
  (:lambda (source)
    (bind (((_ _ field-defs _) source)) field-defs)))

;;
;; csv-target-column-list
;;
;;      iprange ip4r using (ip-range startIpNum endIpNum),
;;
(defrule column-name csv-field-name)	; same rules here
(defrule column-type csv-field-name)	; again, same rules, names only

(defrule column-expression (and kw-using sexp)
  (:lambda (expr)
    (bind (((_ sexp) expr)) sexp)))

(defrule csv-target-column (and column-name
				(? (and ignore-whitespace column-type
					column-expression)))
  (:lambda (col)
    (bind (((name opts)   col)
           ((_ type expr) (or opts (list nil nil nil))))
      (list name type expr))))

(defrule another-csv-target-column (and comma csv-target-column)
  (:lambda (source)
    (bind (((_ col) source)) col)))

(defrule csv-target-columns (and csv-target-column
				 (* another-csv-target-column))
  (:lambda (source)
    (destructuring-bind (col1 cols) source
      (list* col1 cols))))

(defrule target-columns (and kw-target kw-columns) (:constant nil))

(defrule csv-target-column-list (and (? target-columns)
                                     open-paren csv-target-columns close-paren)
  (:lambda (source)
    (bind (((_ _ columns _) source)) columns)))

(defrule csv-target-table (and kw-target kw-table dsn-table-name)
  (:lambda (c-t-t)
    ;; dsn-table-name: (:table-name "schema" . "table")
    (cdr (third c-t-t))))

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
        (bind (((_ _ encoding) enc)) encoding)
	:utf-8)))

(defrule first-filename-matching
    (and (? kw-first) kw-filename kw-matching quoted-regex)
  (:lambda (fm)
    (bind (((_ _ _ regex) fm))
       ;; regex is a list with first the symbol :regex and second the regexp
       ;; as a string
      (list* :regex :first (cdr regex)))))

(defrule all-filename-matching
    (and kw-all (or kw-filenames kw-filename) kw-matching quoted-regex)
  (:lambda (fm)
    (bind (((_ _ _ regex) fm))
      ;; regex is a list with first the symbol :regex and second the regexp
      ;; as a string
      (list* :regex :all (cdr regex)))))

(defrule in-directory (and kw-in kw-directory maybe-quoted-filename)
  (:lambda (in-d)
    (bind (((_ _ dir) in-d)) dir)))

(defrule filename-matching (and (or first-filename-matching
                                    all-filename-matching)
                                (? in-directory))
  (:lambda (filename-matching)
    (bind (((matching directory)        filename-matching)
           (directory                   (or directory `(:filename ,*cwd*)))
           ((m-type first-or-all regex) matching)
           ((d-type dir)                directory)
           (root                        (uiop:directory-exists-p
                                         (if (uiop:absolute-pathname-p dir) dir
                                             (uiop:merge-pathnames* dir *cwd*)))))
      (assert (eq m-type :regex))
      (assert (eq d-type :filename))
      (unless root
        (error "Directory ~s does not exists."
               (uiop:native-namestring dir)))
      `(:regex ,first-or-all ,regex ,root))))

(defrule csv-uri (and "csv://" filename)
  (:lambda (source)
    (bind (((_ filename) source))
      (make-instance 'csv-connection :spec filename))))

(defrule csv-file-source (or stdin
			     inline
                             http-uri
                             csv-uri
			     filename-matching
			     maybe-quoted-filename)
  (:lambda (src)
    (if (typep src 'csv-connection) src
        (destructuring-bind (type &rest specs) src
          (case type
            (:stdin    (make-instance 'csv-connection :spec src))
            (:inline   (make-instance 'csv-connection :spec src))
            (:filename (make-instance 'csv-connection :spec src))
            (:regex    (make-instance 'csv-connection :spec src))
            (:http     (make-instance 'csv-connection :uri (first specs))))))))

(defrule csv-source (and kw-load kw-csv kw-from csv-file-source)
  (:lambda (src)
    (bind (((_ _ _ source) src)) source)))

(defun list-symbols (expression &optional s)
  "Return a list of the symbols used in EXPRESSION."
  (typecase expression
    (symbol  (pushnew expression s))
    (list    (loop for e in expression for s = (list-symbols e s)
		finally (return (reverse s))))
    (t       s)))



(defrule load-csv-file-optional-clauses (* (or csv-options
                                               gucs
                                               before-load
                                               after-load))
  (:lambda (clauses-list)
    (alexandria:alist-plist clauses-list)))

(defrule load-csv-file-command (and csv-source
                                    (? file-encoding) (? csv-source-field-list)
                                    target
                                    (? csv-target-table)
                                    (? csv-target-column-list)
                                    load-csv-file-optional-clauses)
  (:lambda (command)
    (destructuring-bind (source encoding fields pguri table-name columns clauses)
        command
      (list* source
             encoding
             fields
             pguri
             (or table-name (pgconn-table-name pguri))
             columns
             clauses))))

(defun lisp-code-for-csv-dry-run (pg-db-conn)
  `(lambda ()
     ;; CSV connection objects are not actually implementing the generic API
     ;; because they support many complex options... (the file can be a
     ;; pattern or standard input or inline or compressed etc).
     (log-message :log "DRY RUN, only checking PostgreSQL connection.")
     (check-connection ,pg-db-conn)))

(defun lisp-code-for-loading-from-csv (csv-conn pg-db-conn
                                       &key
                                         (encoding :utf-8)
                                         fields
                                         target-table-name
                                         columns
                                         gucs before after options
                                       &allow-other-keys
                                       &aux
                                         (worker-count (getf options :worker-count))
                                         (concurrency  (getf options :concurrency)))
  `(lambda ()
     (let* (,@(pgsql-connection-bindings pg-db-conn gucs)
            ,@(batch-control-bindings options)
              ,@(identifier-case-binding options)
              (source-db (with-stats-collection ("fetch" :section :pre)
                           (expand (fetch-file ,csv-conn)))))

       (progn
         ,(sql-code-block pg-db-conn :pre before "before load")

         (let* ((on-error-stop             (getf ',options :on-error-stop))
                (truncate                  (getf ',options :truncate))
                (disable-triggers          (getf ',options :disable-triggers))
                (drop-indexes              (getf ',options :drop-indexes))
                (max-parallel-create-index (getf ',options :max-parallel-create-index))
                (fields
                 ',(let ((null-as (getf options :null-as)))
                     (if null-as
                         (mapcar (lambda (field)
                                   (if (member :null-as field) field
                                       (append field (list :null-as null-as))))
                                 fields)
                         fields)))
                (source
                 (make-instance 'copy-csv
                                :target-db  ,pg-db-conn
                                :source     source-db
                                :target     (create-table ',target-table-name)
                                :encoding   ,encoding
                                :fields    fields
                                :columns   ',columns
                                ,@(remove-batch-control-option
                                   options :extras '(:null-as
                                                     :worker-count
                                                     :concurrency
                                                     :truncate
                                                     :drop-indexes
                                                     :disable-triggers
                                                     :max-parallel-create-index)))))
           (copy-database source
                          ,@ (when worker-count
                               (list :worker-count worker-count))
                          ,@ (when concurrency
                               (list :concurrency concurrency))
                          :on-error-stop on-error-stop
                          :truncate truncate
                          :drop-indexes drop-indexes
                          :disable-triggers disable-triggers
                          :max-parallel-create-index max-parallel-create-index))

         ,(sql-code-block pg-db-conn :post after "after load")))))

(defrule load-csv-file load-csv-file-command
  (:lambda (command)
    (bind (((source encoding fields pg-db-uri table-name columns
                    &key options gucs before after) command))
      (cond (*dry-run*
             (lisp-code-for-csv-dry-run pg-db-uri))
            (t
             (lisp-code-for-loading-from-csv source pg-db-uri
                                             :encoding encoding
                                             :fields fields
                                             :target-table-name table-name
                                             :columns columns
                                             :gucs gucs
                                             :before before
                                             :after after
                                             :options options))))))

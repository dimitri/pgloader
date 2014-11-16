;;;
;;; LOAD DATABASE FROM SQLite
;;;

(in-package #:pgloader.parser)

#|
load database
     from sqlite:///Users/dim/Downloads/lastfm_tags.db
     into postgresql:///tags

 with drop tables, create tables, create indexes, reset sequences

  set work_mem to '16MB', maintenance_work_mem to '512 MB';
|#
(defrule option-encoding (and kw-encoding encoding)
  (:lambda (enc)
    (cons :encoding
          (if enc
              (destructuring-bind (kw-encoding encoding) enc
                (declare (ignore kw-encoding))
                encoding)
              :utf-8))))

(defrule sqlite-option (or option-batch-rows
                           option-batch-size
                           option-batch-concurrency
                           option-truncate
			   option-data-only
			   option-schema-only
			   option-include-drop
			   option-create-tables
			   option-create-indexes
			   option-reset-sequences
                           option-encoding))

(defrule another-sqlite-option (and comma sqlite-option)
  (:lambda (source)
    (bind (((_ option) source)) option)))

(defrule sqlite-option-list (and sqlite-option (* another-sqlite-option))
  (:lambda (source)
    (destructuring-bind (opt1 opts) source
      (alexandria:alist-plist (list* opt1 opts)))))

(defrule sqlite-options (and kw-with sqlite-option-list)
  (:lambda (source)
    (bind (((_ opts) source))
      (cons :sqlite-options opts))))

(defrule sqlite-db-uri (and "sqlite://" filename)
  (:lambda (source)
    (bind (((_ filename) source)        ; (prefix filename)
           ((_ path)     filename))     ; (type path)
      (list :sqlite path))))

(defrule sqlite-uri (or sqlite-db-uri http-uri maybe-quoted-filename))

(defrule get-sqlite-uri-from-environment-variable (and kw-getenv name)
  (:lambda (p-e-v)
    (bind (((_ varname) p-e-v)
           (connstring  (getenv-default varname)))
      (unless connstring
          (error "Environment variable ~s is unset." varname))
        (parse 'sqlite-uri connstring))))

(defrule sqlite-source (and kw-load kw-database kw-from
                            (or get-sqlite-uri-from-environment-variable
                                sqlite-uri))
  (:lambda (source)
    (bind (((_ _ _ uri) source)) uri)))

(defrule load-sqlite-optional-clauses (* (or sqlite-options
                                             gucs
                                             casts
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
    (bind (((sqlite-uri pg-db-uri
                        &key
                        gucs
                        casts
                        ((:sqlite-options options))
                        ((:including incl))
                        ((:excluding excl)))           source)
           ((&key dbname table-name &allow-other-keys) pg-db-uri))
      `(lambda ()
         (let* ((state-before   (pgloader.utils:make-pgstate))
                (*state*        (pgloader.utils:make-pgstate))
                (*default-cast-rules* ',*sqlite-default-cast-rules*)
                (*cast-rules*         ',casts)
                ,@(pgsql-connection-bindings pg-db-uri gucs)
                ,@(batch-control-bindings options)
                (db
                 ,(bind (((kind url) sqlite-uri))
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
                           (make-pathname :defaults d
                                          :name (pathname-name db)
                                          :type "db"))))
                     db))
                (source
                 (make-instance 'pgloader.sqlite::copy-sqlite
                                :target-db ,dbname
                                :source-db db)))
           (pgloader.sqlite:copy-database
            source
            :state-before state-before
            ,@(when table-name
                    `(:only-tables ',(list table-name)))
            :including ',incl
            :excluding ',excl
            ,@(remove-batch-control-option options)))))))


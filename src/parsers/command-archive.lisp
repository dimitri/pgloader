;;;
;;; LOAD ARCHIVE ...
;;;

(in-package #:pgloader.parser)

(defrule archive-command (or load-csv-file
			     load-dbf-file
			     load-fixed-cols-file))

(defrule another-archive-command (and kw-and archive-command)
  (:lambda (source)
    (bind (((_ col) source)) col)))

(defrule archive-command-list (and archive-command (* another-archive-command))
  (:lambda (source)
    (destructuring-bind (col1 cols) source
      (cons :commands (list* col1 cols)))))

(defrule archive-source (and kw-load kw-archive kw-from filename-or-http-uri)
  (:lambda (src)
    (bind (((_ _ _ source) src)) source)))

(defrule load-archive-clauses (and archive-source
                                   (? target)
                                   (? before-load)
                                   archive-command-list
                                   (? finally))
  (:lambda (command)
    (bind (((source target before commands finally) command)
           ((&key before commands finally)
            (alexandria:alist-plist (remove-if #'null
                                               (list before commands finally)))))
      (list source target
            :before before
            :commands commands
            :finally finally))))

(defrule load-archive load-archive-clauses
  (:lambda (archive)
    (destructuring-bind (source pg-db-conn &key before commands finally) archive
      (when (and (or before finally) (null pg-db-conn))
        (error "When using a BEFORE LOAD DO or a FINALLY block, you must provide an archive level target database connection."))
      `(lambda ()
         (let* (,@(pgsql-connection-bindings pg-db-conn nil)
                (archive-file
                 , (destructuring-bind (kind url) source
                     (ecase kind
                       (:http     `(with-stats-collection
                                       ("download" :section :pre)
                                       (pgloader.archive:http-fetch-file ,url)))
                       (:filename url))))
                  (*fd-path-root*
                   (with-stats-collection ("extract" :section :pre)
                       (pgloader.archive:expand-archive archive-file))))
           (progn
             ,(sql-code-block pg-db-conn :pre before "before load")

             ;; import from files block
             ,@(loop for command in commands
                  collect `(funcall ,command))

             ,(sql-code-block pg-db-conn :post finally "finally")))))))

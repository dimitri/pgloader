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
    (destructuring-bind (source pg-db-uri &key before commands finally) archive
      (when (and (or before finally) (null pg-db-uri))
        (error "When using a BEFORE LOAD DO or a FINALLY block, you must provide an archive level target database connection."))
      (destructuring-bind (&key dbname &allow-other-keys) pg-db-uri
        `(lambda ()
           (let* ((state-before   (pgloader.utils:make-pgstate))
                  (*state*        (pgloader.utils:make-pgstate))
                  ,@(pgsql-connection-bindings pg-db-uri nil)
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

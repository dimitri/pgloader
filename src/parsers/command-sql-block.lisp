#|
       BEFORE LOAD DO $$ sql $$

        LOAD CSV FROM '*/GeoLiteCity-Blocks.csv'      ...
        LOAD DBF FROM '*/GeoLiteCity-Location.csv'    ...

       FINALLY DO     $$ sql $$;
|#

(in-package :pgloader.parser)

(defrule double-dollar (and ignore-whitespace #\$ #\$ ignore-whitespace)
  (:constant :double-dollars))

(defrule dollar-quoted (and double-dollar (* (not double-dollar)) double-dollar)
  (:lambda (dq)
    (bind (((_ quoted _) dq))
      (text quoted))))

(defrule another-dollar-quoted (and comma dollar-quoted)
  (:lambda (source)
    (bind (((_ quoted) source)) quoted)))

(defrule dollar-quoted-list (and dollar-quoted (* another-dollar-quoted))
  (:lambda (source)
    (destructuring-bind (dq1 dqs) source
      (list* dq1 dqs))))

(defrule sql-file (or maybe-quoted-filename)
  (:lambda (filename)
    (destructuring-bind (kind path) filename
      (ecase kind
        (:filename
         (pgloader.sql:read-queries (uiop:merge-pathnames* path *cwd*)))))))

(defrule load-do (and kw-do dollar-quoted-list)
  (:lambda (bld)
    (destructuring-bind (do quoted) bld
      (declare (ignore do))
      quoted)))

(defrule load-execute (and kw-execute sql-file)
  (:lambda (ble)
    (bind (((_ sql) ble)) sql)))

(defrule before-load (and kw-before kw-load (+ (or load-do load-execute)))
  (:lambda (before)
    (bind (((_ _ sql-list-of-list) before))
      (cons :before (apply #'append sql-list-of-list)))))

(defrule finally (and kw-finally (+ (or load-do load-execute)))
  (:lambda (finally)
    (bind (((_ sql-list-of-list) finally))
      (cons :finally (apply #'append sql-list-of-list)))))

(defrule after-load (and kw-after kw-load (+ (or load-do load-execute)))
  (:lambda (after)
    (bind (((_ _ sql-list-of-list) after))
      (cons :after (apply #'append sql-list-of-list)))))

(defrule after-schema (and kw-after kw-create kw-schema
                           (+ (or load-do load-execute)))
  (:lambda (after)
    (bind (((_ _ _ sql-list-of-list) after))
      (cons :after-schema (apply #'append sql-list-of-list)))))

(defun sql-code-block (pgconn section commands label)
  "Return lisp code to run COMMANDS against DBNAME, updating STATE."
  (when commands
    `(execute-sql-code-block ,pgconn ,section ',commands ,label)))

(defun execute-sql-code-block (pgconn section commands label)
  "Exceute given SQL commands."
  (with-stats-collection (label
                          :dbname (db-name pgconn)
                          :section section
                          :use-result-as-read t
                          :use-result-as-rows t)
    (log-message :notice "Executing SQL block for ~a" label)
    (with-pgsql-transaction (:pgconn pgconn)
      (loop :for command :in commands
         :do (pgsql-execute command :client-min-messages :error)
         :counting command))))

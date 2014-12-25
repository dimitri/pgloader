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

(defrule before-load-do (and kw-before kw-load kw-do dollar-quoted-list)
  (:lambda (bld)
    (destructuring-bind (before load do quoted) bld
      (declare (ignore before load do))
      quoted)))

(defrule sql-file (or maybe-quoted-filename)
  (:lambda (filename)
    (destructuring-bind (kind path) filename
      (ecase kind
        (:filename
         (pgloader.sql:read-queries (uiop:merge-pathnames* path *cwd*)))))))

(defrule before-load-execute (and kw-before kw-load kw-execute sql-file)
  (:lambda (ble)
    (bind (((_ _ _ sql) ble)) sql)))

(defrule before-load (or before-load-do before-load-execute)
  (:lambda (before)
    (cons :before before)))

(defrule finally-do (and kw-finally kw-do dollar-quoted-list)
  (:lambda (fd)
    (bind (((_ _ quoted) fd)) quoted)))

(defrule finally-execute (and kw-finally kw-execute sql)
  (:lambda (fe)
    (bind (((_ _ sql) fe)) sql)))

(defrule finally (or finally-do finally-execute)
  (:lambda (finally)
    (cons :finally finally)))

(defrule after-load-do (and kw-after kw-load kw-do dollar-quoted-list)
  (:lambda (fd)
    (bind (((_ _ _ quoted) fd)) quoted)))

(defrule after-load-execute (and kw-after kw-load kw-execute sql-file)
  (:lambda (fd)
    (bind (((_ _ _ sql) fd)) sql)))

(defrule after-load (or after-load-do after-load-execute)
  (:lambda (after)
    (cons :after after)))

(defun sql-code-block (pgconn state commands label)
  "Return lisp code to run COMMANDS against DBNAME, updating STATE."
  (when commands
    `(with-stats-collection (,label
                             :dbname ,(db-name pgconn)
                             :state ,state
                             :use-result-as-read t
                             :use-result-as-rows t)
       (with-pgsql-transaction (:pgconn ,pgconn)
	 (loop for command in ',commands
	    do
	      (log-message :notice command)
	      (pgsql-execute command :client-min-messages :error)
            counting command)))))

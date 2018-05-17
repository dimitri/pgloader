;;;
;;; Regression tests driver.
;;;
;;; We're using SQL EXCEPT to compare what we loaded with what we expected
;;; to load.
;;;

(in-package #:pgloader)

(define-condition regression-test-error (error)
  ((filename :initarg :filename :reader regression-test-filename))
  (:report (lambda (err stream)
             (format stream
                     "Regression test failed: ~s"
                     (regression-test-filename err)))))

(defun process-regression-test (load-file &key start-logger)
  "Run a regression test for given LOAD-FILE."
  (unless (probe-file load-file)
    (format t "Regression testing ~s: file does not exists." load-file)
    #-pgloader-image (values nil +os-code-error-regress+)
    #+pgloader-image (uiop:quit +os-code-error-regress+))

  ;; now do our work
  (with-monitor (:start-logger start-logger)
    (log-message :log "Regression testing: ~s" load-file)
    (process-command-file (list load-file) :flush-summary nil)

    ;; once we are done running the load-file, compare the loaded data with
    ;; our expected data file
    (bind ((expected-data-source
            (regression-test-expected-data-source load-file))

           ((target-conn target-table-name gucs)
            (parse-target-pg-db-uri load-file))

           (target-table (create-table target-table-name))
           (*pg-settings* (pgloader.pgsql:sanitize-user-gucs gucs))
           (*pgsql-reserved-keywords* (list-reserved-keywords target-conn))

           ;; change target table-name schema
           (expected-data-target
            (let ((e-d-t (clone-connection target-conn)))
              (setf (pgconn-table-name e-d-t)
                    ;;
                    ;; The connection facility still works with cons here,
                    ;; rather than table structure instances, because of
                    ;; depedencies as explained in
                    ;; src/parsers/command-db-uri.lisp
                    ;;
                    (cons "expected" (table-name target-table)))
              e-d-t))
           (expected-target-table
            (create-table (cons "expected" (table-name target-table)))))

      (log-message :log "Comparing loaded data against ~s"
                   (cdr (pgloader.sources::md-spec expected-data-source)))

      ;; prepare expected table in "expected" schema
      (with-pgsql-connection (target-conn)
        (with-schema (unqualified-table-name target-table)
          (let* ((tname  (apply-identifier-case unqualified-table-name))
                 (drop   (format nil "drop table if exists expected.~a;"
                                 tname))
                 (create (format nil "create table expected.~a(like ~a);"
                                 tname tname)))
            (log-message :notice "~a" drop)
            (pomo:query drop)
            (log-message :notice "~a" create)
            (pomo:query create))))

      ;; load expected data
      (load-data :from expected-data-source
                 :into expected-data-target
                 :target-table-name expected-target-table
                 :options '(:truncate t)
                 :start-logger nil
                 :flush-summary t)

      ;; now compare both
      (with-pgsql-connection (target-conn)
        (with-schema (unqualified-table-name target-table)
          (let* ((tname  (apply-identifier-case unqualified-table-name))
                 (cols (loop :for (name type)
                          :in (list-columns tname)
                          ;;
                          ;; We can't just use table names here, because
                          ;; PostgreSQL support for the POINT datatype fails
                          ;; to implement EXCEPT support, and the query then
                          ;; fails with:
                          ;;
                          ;; could not identify an equality operator for type point
                          ;;
                          :collect (if (string= "point" type)
                                       (format nil "~s::text" name)
                                       (format nil "~s" name))))
                 (sql  (format nil
                               "select count(*) from (select ~{~a~^, ~} from expected.~a except select ~{~a~^, ~} from ~a) ss"
                               cols
                               tname
                               cols
                               tname))
                 (diff-count (pomo:query sql :single)))
            (log-message :notice "~a" sql)
            (log-message :notice "Got a diff of ~a rows" diff-count)

            ;; signal a regression test error when diff isn't 0
            (unless (zerop diff-count)
              (error 'regression-test-error :filename load-file))

            (log-message :log "Regress pass.")
            (values diff-count +os-code-success+)))))))


;;;
;;; TODO: use the catalogs structures and introspection facilities.
;;;
(defun list-columns (table-name &optional schema)
  "Returns the list of columns for table TABLE-NAME in schema SCHEMA, and
   must be run with an already established PostgreSQL connection."
  (pomo:query (format nil "
    select attname, t.oid::regtype
      from pg_class c
           join pg_namespace n on n.oid = c.relnamespace
           left join pg_attribute a on c.oid = a.attrelid
           join pg_type t on t.oid = a.atttypid
     where c.oid = '~:[~*~a~;~a.~a~]'::regclass and attnum > 0
  order by attnum" schema schema table-name)))


;;;
;;; Helper functions
;;;
(defun regression-test-expected-data-source (load-file)
  "Returns the source specification where to read the expected result for
   the given LOAD-FILE."

  (let* ((load-file-dir        (uiop:pathname-directory-pathname
                                (if (uiop:absolute-pathname-p load-file)
                                    load-file
                                    (uiop:merge-pathnames* load-file
                                                           (uiop:getcwd)))))
         (expected-subdir      (uiop:native-namestring
                                (uiop:merge-pathnames* "regress/expected/"
                                                       load-file-dir)))
         (expected-data-file   (make-pathname :defaults load-file
                                              :type "out"
                                              :directory expected-subdir))
         (expected-data-source (uiop:native-namestring expected-data-file)))

    (parse-source-string-for-type :copy expected-data-source)))


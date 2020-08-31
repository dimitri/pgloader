;;;
;;; Tools to handle MySQL data fetching
;;;

(in-package :pgloader.source.mysql)

;;;
;;; Implement the specific methods
;;;
(defmethod concurrency-support ((mysql copy-mysql) concurrency)
  "Splits the read work thanks WHERE clauses when possible and relevant,
   return nil if we decide to read all in a single thread, and a list of as
   many copy-mysql instances as CONCURRENCY otherwise. Each copy-mysql
   instance in the returned list embeds specifications about how to read
   only its partition of the source data."
  (unless (= 1 concurrency)
    (let* ((indexes (table-index-list (target mysql)))
           (pkey    (first (remove-if-not #'index-primary indexes)))
           (pcol    (when pkey (first (index-columns pkey))))
           (coldef  (when pcol
                      (find pcol
                            (table-column-list (target mysql))
                            :key #'column-name
                            :test #'string=)))
           (ptype   (when (and coldef (stringp (column-type-name coldef)))
                      (column-type-name coldef))))
      (when (member ptype (list "integer" "bigint" "serial" "bigserial")
                    :test #'string=)
        ;; the table has a primary key over a integer data type we are able
        ;; to generate WHERE clause and range index scans.
        (with-connection (*connection* (source-db mysql))
          (let* ((col (mysql-column-name
                       (nth (position coldef (table-column-list (target mysql)))
                            (fields mysql))))
                 (sql (format nil "select min(`~a`), max(`~a`) + 1 from `~a`"
                              col col (table-source-name (source mysql)))))
            (destructuring-bind (min max)
                (let ((result (first (mysql-query sql))))
                  ;; result is (min max), or (nil nil) if table is empty
                  (if (or (null (first result))
                          (null (second result)))
                      result
                      (mapcar #'parse-integer result)))
              ;; generate a list of ranges from min to max
              (when (and min max)
                (let ((range-list (split-range min max *rows-per-range*)))
                  (unless (< (length range-list) concurrency)
                    ;; affect those ranges to each reader, we have CONCURRENCY
                    ;; of them
                    (let ((partitions (distribute range-list concurrency)))
                      (loop :for part :in partitions :collect
                         (make-instance 'copy-mysql
                                        :source-db  (clone-connection
                                                     (source-db mysql))
                                        :target-db  (target-db mysql)
                                        :source     (source mysql)
                                        :target     (target mysql)
                                        :fields     (fields mysql)
                                        :columns    (columns mysql)
                                        :transforms (transforms mysql)
                                        :encoding   (encoding mysql)
                                        :range-list (cons col part))))))))))))))

(defun call-with-encoding-handler (copy-mysql table-name func)
  (handler-bind
      ;; Newer versions of qmynd handle the babel error and signal this one
      ;; with more details and an improved reporting:
      ((qmynd-impl::decoding-error
        #'(lambda (c)
            (update-stats :data (target copy-mysql) :errs 1)
            (log-message :error "~a" c)
            (invoke-restart 'qmynd-impl::use-nil)))

       ;; Older versions of qmynd reported babel errors directly
       (babel-encodings:end-of-input-in-character
        #'(lambda (c)
            (update-stats :data (target copy-mysql) :errs 1)
            (log-message :error "~a" c)
            (invoke-restart 'qmynd-impl::use-nil)))

       (babel-encodings:character-decoding-error
        #'(lambda (c)
            (update-stats :data (target copy-mysql) :errs 1)
            (let* ((encoding (babel-encodings:character-coding-error-encoding c))
                   (position (babel-encodings:character-coding-error-position c))
                   (buffer   (babel-encodings:character-coding-error-buffer c))
                   (character
                    (when (and position (< position (length buffer)))
                      (aref buffer position))))
              (log-message :error
                           "While decoding text data from MySQL table ~s: ~%~
Illegal ~a character starting at position ~a~@[: ~a~].~%"
                           table-name encoding position character))
            (invoke-restart 'qmynd-impl::use-nil))))
    (funcall func)))

(defmacro with-encoding-handler ((copy-mysql table-name) &body forms)
  `(call-with-encoding-handler ,copy-mysql ,table-name (lambda () ,@forms)))

(defmethod map-rows ((mysql copy-mysql) &key process-row-fn)
  "Extract MySQL data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row."
  (let ((table-name (table-source-name (source mysql)))
        (qmynd:*mysql-encoding*
         (when (encoding mysql)
           #+sbcl (encoding mysql)
           #+ccl  (ccl:external-format-character-encoding (encoding mysql)))))

    (with-connection (*connection* (source-db mysql))
      (when qmynd:*mysql-encoding*
        (log-message :notice "Force encoding to ~a for ~a"
                     qmynd:*mysql-encoding* table-name))
      (let* ((cols (get-column-list mysql))
             (sql  (format nil "SELECT ~{~a~^, ~} FROM `~a`" cols table-name)))

        (if (range-list mysql)
            ;; read a range at a time, in a loop
            (destructuring-bind (colname . ranges) (range-list mysql)
              (loop :for (min max) :in ranges :do
                 (let ((sql (format nil "~a WHERE `~a` >= ~a AND `~a` < ~a"
                                    sql colname min colname max)))
                   (with-encoding-handler (mysql table-name)
                     (mysql-query sql
                                  :row-fn process-row-fn
                                  :result-type 'vector)))))

            ;; read it all, no WHERE clause
            (with-encoding-handler (mysql table-name)
              (mysql-query sql
                           :row-fn process-row-fn
                           :result-type 'vector)))))))



(defmethod copy-column-list ((mysql copy-mysql))
  "We are sending the data in the MySQL columns ordering here."
  (mapcar #'apply-identifier-case (mapcar #'mysql-column-name (fields mysql))))


(defmethod fetch-metadata ((mysql copy-mysql)
                           (catalog catalog)
                           &key
                             materialize-views
                             (create-indexes   t)
                             (foreign-keys     t)
                             including
                             excluding)
  "MySQL introspection to prepare the migration."
  (let ((schema        (add-schema catalog (catalog-name catalog)
                                   :in-search-path t))
        (including     (filter-list-to-where-clause mysql including))
        (excluding     (filter-list-to-where-clause mysql excluding :not t)))
    (with-stats-collection ("fetch meta data"
                            :use-result-as-rows t
                            :use-result-as-read t
                            :section :pre)
      (with-connection (*connection* (source-db mysql))
        ;; If asked to MATERIALIZE VIEWS, now is the time to create them in
        ;; MySQL, when given definitions rather than existing view names.
        (when (and materialize-views (not (eq :all materialize-views)))
          (create-matviews materialize-views mysql))

        ;; fetch table and columns metadata, covering table and column comments
        (fetch-columns schema mysql
                       :including including
                       :excluding excluding)

        ;; fetch tables row count estimate
        (fetch-table-row-count schema mysql
                               :including including
                               :excluding excluding)

        ;; fetch view (and their columns) metadata, covering comments too
        (let* ((view-names (unless (eq :all materialize-views)
                             (mapcar #'matview-source-name materialize-views)))
               (including
                (loop :for (schema-name . view-name) :in view-names
                   :collect (make-string-match-rule :target view-name)))
               (including-clause (filter-list-to-where-clause mysql including)))
          (cond (view-names
                 (fetch-columns schema mysql
                                :including including-clause
                                :excluding excluding
                                :table-type :view))

                ((eq :all materialize-views)
                 (fetch-columns schema mysql :table-type :view))))

        (when foreign-keys
          (fetch-foreign-keys schema mysql
                              :including including
                              :excluding excluding))

        (when create-indexes
          (fetch-indexes schema mysql
                         :including including
                         :excluding excluding))

        ;; return how many objects we're going to deal with in total
        ;; for stats collection
        (+ (count-tables catalog)
           (count-views catalog)
           (count-indexes catalog)
           (count-fkeys catalog))))

    catalog))

(defmethod cleanup ((mysql copy-mysql) (catalog catalog) &key materialize-views)
  "When there is a PostgreSQL error at prepare-pgsql-database step, we might
   need to clean-up any view created in the MySQL connection for the
   migration purpose."
  (when materialize-views
    (with-connection (*connection* (source-db mysql))
      (drop-matviews materialize-views mysql))))

(defvar *decoding-as* nil
  "Special per-table encoding/decoding overloading rules for MySQL.")

(defun apply-decoding-as-filters (table-name filters)
  "Return a generialized boolean which is non-nil only if TABLE-NAME matches
   one of the FILTERS."
  (flet ((apply-filter (filter) (matches filter table-name)))
    (some #'apply-filter filters)))

(defmethod instanciate-table-copy-object ((copy copy-mysql) (table table))
  "Create an new instance for copying TABLE data."
  (let ((new-instance (change-class (call-next-method copy table) 'copy-mysql)))
    (setf (encoding new-instance)
          ;; force the data encoding when asked to
          (when *decoding-as*
            (loop :for (encoding . filters) :in *decoding-as*
               :when (apply-decoding-as-filters (table-name table) filters)
               :return encoding)))
    new-instance))


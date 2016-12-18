;;;
;;; Tools to handle MySQL data fetching
;;;

(in-package :pgloader.mysql)

(defclass copy-mysql (db-copy)
  ((encoding :accessor encoding         ; allows forcing encoding
             :initarg :encoding
             :initform nil))
  (:documentation "pgloader MySQL Data Source"))

(defmethod initialize-instance :after ((source copy-mysql) &key)
  "Add a default value for transforms in case it's not been provided."
  (let ((transforms (and (slot-boundp source 'transforms)
                         (slot-value  source 'transforms))))
    (when (and (slot-boundp source 'fields) (slot-value source 'fields))
      ;; cast typically happens in copy-database in the schema structure,
      ;; and the result is then copied into the copy-mysql instance.
      (unless (and (slot-boundp source 'columns) (slot-value source 'columns))
        (setf (slot-value source 'columns)
              (mapcar #'cast (slot-value source 'fields))))

      (unless transforms
        (setf (slot-value source 'transforms)
              (mapcar #'column-transform (slot-value source 'columns)))))))


;;;
;;; Implement the specific methods
;;;
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
      (let* ((cols (get-column-list (db-name (source-db mysql)) table-name))
             (sql  (format nil "SELECT ~{~a~^, ~} FROM `~a`;" cols table-name)))
        (handler-bind
            ;; avoid trying to fetch the character at end-of-input position...
            ((babel-encodings:end-of-input-in-character
              #'(lambda (c)
                  (update-stats :data (target mysql) :errs 1)
                  (log-message :error "~a" c)
                  (invoke-restart 'qmynd-impl::use-nil)))
             (babel-encodings:character-decoding-error
              #'(lambda (c)
                  (update-stats :data (target mysql) :errs 1)
                  (let ((encoding (babel-encodings:character-coding-error-encoding c))
                        (position (babel-encodings:character-coding-error-position c))
                        (character
                         (aref (babel-encodings:character-coding-error-buffer c)
                               (babel-encodings:character-coding-error-position c))))
                    (log-message :error
                                 "~a: Illegal ~a character starting at position ~a: ~a."
                                 table-name encoding position character))
                  (invoke-restart 'qmynd-impl::use-nil))))
          (mysql-query sql :row-fn process-row-fn :result-type 'vector))))))



(defmethod copy-column-list ((mysql copy-mysql))
  "We are sending the data in the MySQL columns ordering here."
  (mapcar #'apply-identifier-case (mapcar #'mysql-column-name (fields mysql))))


(defmethod fetch-metadata ((mysql copy-mysql)
                           (catalog catalog)
                           &key
                             materialize-views
                             only-tables
                             (create-indexes   t)
                             (foreign-keys     t)
                             including
                             excluding)
  "MySQL introspection to prepare the migration."
  (let ((schema        (add-schema catalog (catalog-name catalog)))
        (view-names    (unless (eq :all materialize-views)
                         (mapcar #'car materialize-views))))
    (with-stats-collection ("fetch meta data"
                            :use-result-as-rows t
                            :use-result-as-read t
                            :section :pre)
        (with-connection (*connection* (source-db mysql))
          ;; If asked to MATERIALIZE VIEWS, now is the time to create them in
          ;; MySQL, when given definitions rather than existing view names.
          (when (and materialize-views (not (eq :all materialize-views)))
            (create-my-views materialize-views))

          ;; fetch table and columns metadata, covering table and column comments
          (list-all-columns schema
                            :only-tables only-tables
                            :including including
                            :excluding excluding)

          ;; fetch view (and their columns) metadata, covering comments too
          (cond (view-names (list-all-columns schema
                                              :only-tables view-names
                                              :table-type :view))

                ((eq :all materialize-views)
                 (list-all-columns schema :table-type :view)))

          (when foreign-keys
            (list-all-fkeys schema
                            :only-tables only-tables
                            :including including
                            :excluding excluding))

          (when create-indexes
            (list-all-indexes schema
                              :only-tables only-tables
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
      (drop-my-views materialize-views))))

(defvar *decoding-as* nil
  "Special per-table encoding/decoding overloading rules for MySQL.")

(defun apply-decoding-as-filters (table-name filters)
  "Return a generialized boolean which is non-nil only if TABLE-NAME matches
   one of the FILTERS."
  (flet ((apply-filter (filter)
           ;; we close over table-name here.
           (typecase filter
             (string (string-equal filter table-name))
             (list   (destructuring-bind (type val) filter
                       (ecase type
                         (:regex (cl-ppcre:scan val table-name))))))))
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


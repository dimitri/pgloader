;;;
;;; Tools to handle the SQLite Database
;;;

(in-package :pgloader.source.sqlite)

;;; Map a function to each row extracted from SQLite
;;;
(declaim (inline parse-value))

(defun parse-value (value sqlite-type pgsql-type &key (encoding :utf-8))
  "Parse value given by SQLite to match what PostgreSQL is expecting.
   In some cases SQLite will give text output for a blob column (it's
   base64) and at times will output binary data for text (utf-8 byte
   vector)."
  (cond ((and (string-equal "text" pgsql-type)
              (eq :blob sqlite-type)
              (not (stringp value)))
         ;; we expected a properly encoded string and received bytes instead
         (babel:octets-to-string value :encoding encoding))

        ((and (string-equal "bytea" pgsql-type)
              (stringp value))
         ;; we expected bytes and got a string instead, must be base64 encoded
         (base64:base64-string-to-usb8-array value))

        ;; default case, just use what's been given to us
        (t value)))

(defmethod map-rows ((sqlite copy-sqlite) &key process-row-fn)
  "Extract SQLite data and call PROCESS-ROW-FN function with a single
   argument (a list of column values) for each row"
  (let* ((table-name (table-source-name (source sqlite)))
         (cols       (mapcar #'coldef-name (fields sqlite)))
         (sql        (format nil "SELECT ~{`~a`~^, ~} FROM `~a`;" cols table-name))
         (pgtypes    (map 'vector #'column-type-name (columns sqlite))))
    (log-message :sql "SQLite: ~a" sql)
    (with-connection (*sqlite-db* (source-db sqlite))
      (let* ((db (conn-handle *sqlite-db*))
             (encoding (sqlite-encoding db)))
        (handler-case
            (loop
               with statement = (sqlite:prepare-statement db sql)
               with len = (loop :for name
                             :in (sqlite:statement-column-names statement)
                             :count name)
               while (sqlite:step-statement statement)
               for row = (let ((v (make-array len)))
                           (loop :for x :below len
                              :for raw := (sqlite:statement-column-value statement x)
                              :for ptype := (aref pgtypes x)
                              :for stype := (sqlite-ffi:sqlite3-column-type
                                             (sqlite::handle statement)
                                             x)
                              :for val := (parse-value raw stype ptype
                                                       :encoding encoding)
                              :do (setf (aref v x) val))
                           v)
               counting t into rows
               do (funcall process-row-fn row)
               finally
                 (sqlite:finalize-statement statement)
                 (return rows))
          (condition (e)
            (log-message :error "~a" e)
            (update-stats :data (target sqlite) :errs 1)))))))

(defmethod copy-column-list ((sqlite copy-sqlite))
  "Send the data in the SQLite column ordering."
  (mapcar #'apply-identifier-case (mapcar #'coldef-name (fields sqlite))))

(defmethod fetch-metadata ((sqlite copy-sqlite) (catalog catalog)
                           &key
                             materialize-views
                             only-tables
                             (create-indexes t)
                             (foreign-keys   t)
                             including
                             excluding)
  "SQLite introspection to prepare the migration."
  (declare (ignore materialize-views only-tables))
  (let ((schema (add-schema catalog nil)))
    (with-stats-collection ("fetch meta data"
                            :use-result-as-rows t
                            :use-result-as-read t
                            :section :pre)
      (with-connection (conn (source-db sqlite) :check-has-sequences t)
        (let ((*sqlite-db* (conn-handle conn)))
          (fetch-columns schema
                         sqlite
                         :including including
                         :excluding excluding
                         :db-has-sequences (has-sequences conn))

          (when create-indexes
            (fetch-indexes schema sqlite))

          (when foreign-keys
            (fetch-foreign-keys schema sqlite)))

        ;; return how many objects we're going to deal with in total
        ;; for stats collection
        (+ (count-tables catalog)
           (count-indexes catalog)
           (count-fkeys catalog))))
    catalog))



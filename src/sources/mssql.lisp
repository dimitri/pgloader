;;;
;;; Tools to handle the MS SQL Database
;;;

(in-package :pgloader.mssql)

(defclass copy-mssql (copy)
  ((encoding :accessor encoding         ; allows forcing encoding
             :initarg :encoding
             :initform nil))
  (:documentation "pgloader MS SQL Data Source"))

(defun cast-mssql-column-definition-to-pgsql (mssql-column)
  "Return the PostgreSQL column definition from the MySQL one."
  (with-slots (schema table-name name type default nullable)
      mssql-column
    (declare (ignore schema))   ; FIXME
    (let ((ctype (mssql-column-ctype mssql-column)))
      (cast table-name name type ctype default nullable nil))))

(defmethod initialize-instance :after ((source copy-mssql) &key)
  "Add a default value for transforms in case it's not been provided."
  (let* ((source-db  (slot-value source 'source-db))
	 (table-name (when (slot-boundp source 'source)
		       (slot-value source 'source)))
	 (fields     (or (and (slot-boundp source 'fields)
			      (slot-value source 'fields))
			 (when table-name
			   (let* ((all-columns (list-all-columns :dbname source-db)))
			     (cdr (assoc table-name all-columns
					 :test #'string=))))))
	 (transforms (when (slot-boundp source 'transforms)
		       (slot-value source 'transforms))))

    ;; default to using the same database name as source and target
    (when (and source-db
	       (or (not (slot-boundp source 'target-db))
		   (not (slot-value source 'target-db))))
      (setf (slot-value source 'target-db) source-db))

    ;; default to using the same table-name as source and target
    (when (and table-name
	       (or (not (slot-boundp source 'target))
                   (not (slot-value source 'target))))
      (setf (slot-value source 'target) table-name))

    (when fields
      (unless (slot-boundp source 'fields)
	(setf (slot-value source 'fields) fields))

      (loop :for field :in fields
         :for (column fn) := (multiple-value-bind (column fn)
                               (cast-mysql-column-definition-to-pgsql field)
                             (list column fn))
         :collect column :into columns
         :collect fn :into fns
         :finally (progn (setf (slot-value source 'columns) columns)
                        (unless transforms
                          (setf (slot-value source 'transforms) fns)))))))

(defmethod copy-database ((mssql copy-mssql)
                          &key
			    state-before
			    data-only
			    schema-only
			    (truncate        nil)
			    (create-tables   t)
			    (include-drop    t)
			    (create-indexes  t)
			    (reset-sequences t)
			    only-tables
			    including
			    excluding
                            (identifier-case :downcase)
                            (encoding :utf-8))
  "Stream the given MS SQL database down to PostgreSQL."
  (declare (ignore create-indexes reset-sequences))
  (let* ((summary       (null *state*))
	 (*state*       (or *state* (make-pgstate)))
	 (state-before  (or state-before (make-pgstate)))
	 (idx-state     (make-pgstate))
	 (seq-state     (make-pgstate))
         (cffi:*default-foreign-encoding* encoding)
         ;; (copy-kernel   (make-kernel 2))
         (all-columns   (filter-column-list (list-all-columns)
					    :only-tables only-tables
					    :including including
					    :excluding excluding))
         ;; (all-indexes   (filter-column-list (list-all-indexes)
	 ;;        			    :only-tables only-tables
	 ;;        			    :including including
	 ;;        			    :excluding excluding))
         ;; (max-indexes   (loop :for (table . indexes) :in all-indexes
         ;;                   :maximizing (length indexes)))
         ;; (idx-kernel    (when (and max-indexes (< 0 max-indexes))
	 ;;        	  (make-kernel max-indexes)))
         ;; (idx-channel   (when idx-kernel
	 ;;        	  (let ((lp:*kernel* idx-kernel))
	 ;;        	    (lp:make-channel))))
	 ;; (pg-dbname     (target-db mssql))
         )

    ;; if asked, first drop/create the tables on the PostgreSQL side
    (cond ((and (or create-tables schema-only) (not data-only))
           (log-message :notice "~:[~;DROP then ~]CREATE TABLES" include-drop)
           (with-stats-collection ("create, truncate"
                                   :state state-before
                                   :summary summary)
             (with-pgsql-transaction ()
               (loop :for (schema . tables) :in all-columns
                  :do (let ((schema
                             (apply-identifier-case schema identifier-case)))
                        ;; create schema
                        (let ((sql (format nil "CREATE SCHEMA ~a;" schema)))
                          (log-message :notice "~a" sql)
                          (pgsql-execute sql))

                        ;; set search_path to only that schema
                        (pgsql-execute
                         (format nil "SET LOCAL search_path TO ~a;" schema))

                        ;; and now create the tables within that schema
                        (create-tables tables
                                       :include-drop include-drop
                                       :identifier-case identifier-case))))))

          (truncate
           (let ((qualified-table-name-list
                  (qualified-table-name-list all-columns
                                             :identifier-case identifier-case)))
             (truncate-tables *pg-dbname*
                              ;; here we really do want only the name
                              (mapcar #'car qualified-table-name-list)
                              :identifier-case identifier-case))))

    ;; and report the total time spent on the operation
    (report-full-summary "Total streaming time" *state*
                         :before state-before
                         :finally seq-state
                         :parallel idx-state)))

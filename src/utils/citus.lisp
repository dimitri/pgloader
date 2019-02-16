;;;
;;; Citus support in pgloader allows to declare what needs to change in the
;;; source schema in terms of Citus concepts: reference and distributed
;;; table.
;;;

#|
   distribute billers using id
   distribute bills using biller_id
   distribute receivable_accounts using biller_id
   distribute payments using biller_id

   distribute splits using biller_id
                      from receivable_accounts

   distribute ach_accounts as reference table
|#


(in-package #:pgloader.citus)

;;;
;;; Main data structures to host our distribution rules.
;;;
(defstruct citus-reference-rule table)
(defstruct citus-distributed-rule table using from)

(defun citus-distribute-schema (catalog distribution-rules)
  "Distribute a CATALOG with given user provided DISTRIBUTION-RULES. Return
   the list of rules applied."
  (let ((processed-rules '())
        (derived-rules
         (loop :for rule :in distribution-rules
            :append (progn
                      (citus-set-table rule catalog)
                      (compute-foreign-rules rule (citus-rule-table rule))))))

    ;;
    ;; Apply rules only once.
    ;;
    ;; ERROR Database error 42P16: table ;; "campaigns" is already distributed
    ;;
    ;; In the PostgreSQL source case, we have the table OIDs already at this
    ;; point, but in the general case we don't. Use the names to match what
    ;; we did up to now.
    ;;
    (loop :for rule :in (append distribution-rules derived-rules)
       :unless (member (table-source-name (citus-rule-table rule))
                       processed-rules
                       :key (lambda (rule)
                              (table-source-name (citus-rule-table rule)))
                       :test #'equal)
       :collect (progn
                  (push rule processed-rules)
                  (apply-citus-rule rule)
                  rule))))

(define-condition citus-rule-table-not-found (error)
  ((schema-name :initarg :schema-name
                :accessor citus-rule-table-not-found-schema-name)
   (table-name :initarg :table-name
               :accessor citus-rule-table-not-found-table-name))
  (:report
   (lambda (err stream)
     (let ((*print-circle* nil))
       (with-slots (schema-name table-name)
           err
         (format stream
                 "Could not find table ~s in schema ~s for distribution rules."
                 table-name schema-name))))))

(defun citus-find-table (catalog table)
  (let* ((source-name (table-source-name table))
         (table-name  (etypecase source-name
                        (string source-name)
                        (cons   (cdr source-name))))
         (schema-name (schema-name (table-schema table))))
    (or (find-table (find-schema catalog schema-name) table-name)
        (error (make-condition 'citus-rule-table-not-found
                               :table-name table-name
                               :schema-name schema-name)))))

(defgeneric citus-rule-table (rule)
  (:documentation "Returns the RULE's table.")
  (:method ((rule citus-reference-rule))   (citus-reference-rule-table rule))
  (:method ((rule citus-distributed-rule)) (citus-distributed-rule-table rule)))

(defgeneric citus-set-table (rule catalog)
  (:documentation "Find citus RULE table in CATALOG and update the
  placeholder with the table found there.")
  (:method ((rule citus-reference-rule) (catalog catalog))
    (let ((table (citus-reference-rule-table rule)))
      (setf (citus-reference-rule-table rule)
            (citus-find-table catalog table))))

  (:method ((rule citus-distributed-rule) (catalog catalog))
    (let ((table (citus-distributed-rule-table rule)))
      (map-into (citus-distributed-rule-from rule)
                (lambda (from) (citus-find-table catalog from))
                (citus-distributed-rule-from rule))
      (setf (citus-distributed-rule-table rule)
            (citus-find-table catalog table)))))

(defmethod print-object ((rule citus-reference-rule) stream)
  (print-unreadable-object (rule stream :type t :identity t)
    (with-slots (table) rule
      (format stream
              "distribute ~a as reference"
              (format-table-name table)))))

(defmethod print-object ((rule citus-distributed-rule) stream)
  (print-unreadable-object (rule stream :type t :identity t)
    (with-slots (table using from) rule
      (format stream
              "distribute ~a :using ~a~@[ :from ~{~a~^, ~}~]"
              (format-table-name table)
              (column-name using)
              (mapcar #'format-table-name from)))))


;;;
;;; When distributing a table on a given key, we can follow foreign keys
;;; pointing to this table. We might find out that when computing the
;;; following rule:
;;;
;;;    distribute companies using id
;;;
;;; We then want to add the set of rules that we find walking the foreign
;;; keys:
;;;
;;;   distribute campaigns using company_id
;;;   distribute ads using company_id from campaigns
;;;   distribute clicks using company_id from ads, campaigns
;;;   distribute impressions using company_id from ads, campaigns
;;;
(defgeneric compute-foreign-rules (rule table &key)
  (:documentation
   "Compute rules to apply that derive from the distribution rule RULE when
    following foreign-keys from TABLE."))

(defmethod compute-foreign-rules ((rule citus-reference-rule)
                                  (table table)
                                  &key)
  "There's nothing to do here, reference table doesn't impact the schema."
  nil)

(defmethod compute-foreign-rules ((rule citus-distributed-rule)
                                  (table table)
                                  &key fkey-list)
  "Find every foreign key that points to TABLE and add return a list of new
   rules for the source of those foreign keys."
  (let ((pkey  (find-if #'index-primary (table-index-list table))))

    (when (and pkey (member (column-name (citus-distributed-rule-using rule))
                            (index-columns pkey)
                            :test #'string=))
      (loop :for fkey :in (index-fk-deps pkey)
         :for new-fkey-list := (cons fkey fkey-list)
         :for new-rule := (make-distributed-table-from-fkey rule new-fkey-list)
         :collect new-rule :into new-rule-list
         :collect (compute-foreign-rules rule (fkey-table fkey)
                                         :fkey-list new-fkey-list)
         :into dep-rule-list
         :finally (return (append new-rule-list
                                  ;; flatten sub-lists as we go
                                  (apply #'append dep-rule-list)))))))

(defun make-distributed-table-from-fkey (rule fkey-list)
  "Make a new Citus distributed table rule from an existing rule and a fkey
   definition."
  ;;
  ;; We have a list of foreign keys pointing from a current table,
  ;; (fkey-table fkey), to the root table that is distributed,
  ;; (fkey-foreign-table fkey).
  ;;
  ;; For the distribution key name, we consider the name of the column used
  ;; in the last entry from the fkey-list, the column name that points to
  ;; the root.id distribution key and might be named root_id or something.
  ;;
  ;; Then we only need to specifying USING the intermediate tables, the last
  ;; entry gives us the data we need to backfill our tables.
  ;;
  (let* ((fkey     (car (last fkey-list)))
         (dist-key (column-name (citus-distributed-rule-using rule)))
         (dist-key-pos (position dist-key
                                 (fkey-foreign-columns fkey)
                                 :test #'string=))
         (fkey-table-dist-key (nth dist-key-pos (fkey-columns fkey)))
         (from-table-list (butlast (mapcar #'fkey-foreign-table fkey-list))))
    (make-citus-distributed-rule :table (fkey-table (first fkey-list))
                                  :using (make-column :name fkey-table-dist-key)
                                  :from from-table-list)))


;;;
;;; Apply a citus distribution rule to given table, and store the rule
;;; itself to the table-citus-rule slot so that we later know to generate a
;;; proper SELECT query that includes the backfilling.
;;;
(define-condition citus-rule-is-missing-from-list (error)
  ((rule  :initarg :rule :accessor citus-rule))
  (:report
   (lambda (err stream)
     (let ((*print-circle* nil))
       (format stream
               "Failed to add column ~s to table ~a for lack of a FROM clause in the distribute rule:~%    distribute ~a using ~a from ?"
               (column-name (citus-distributed-rule-using (citus-rule err)))
               (format-table-name (citus-distributed-rule-table (citus-rule err)))
               (format-table-name (citus-distributed-rule-table (citus-rule err)))
               (column-name (citus-distributed-rule-using (citus-rule err))))))))

(defgeneric apply-citus-rule (rule)
  (:documentation "Apply a Citus distribution RULE to given TABLE."))

(defmethod apply-citus-rule ((rule citus-reference-rule))
  ;; for a reference table, we have nothing to do really.
  (setf (table-citus-rule (citus-reference-rule-table rule)) rule)
  t)

(defmethod apply-citus-rule ((rule citus-distributed-rule))
  ;; ok now we need to check if the USING column exists or if we need to add
  ;; it to our model
  (setf (table-citus-rule (citus-distributed-rule-table rule)) rule)

  (let* ((table  (citus-distributed-rule-table rule))
         (column (find (column-name (citus-distributed-rule-using rule))
                       (table-field-list table)
                       :test #'string=
                       :key #'field-name)))
    (if column

        ;; add it to the PKEY definition, in first position
        (add-column-to-pkey table
                            (column-name (citus-distributed-rule-using rule)))

        ;; The column doesn't exist, we need to find it in the :FROM rule's
        ;; list. The :FROM slot of the rule is a list of tables to
        ;; "traverse" when backfilling the data. The list follows the
        ;; foreign-key relationships from TABLE to the source of the
        ;; distribution key.
        ;;
        ;; To find the column definition to add to the current TABLE, look
        ;; it up in the last entry of the FROM rule's list.
        (let* ((last-from-rule (car (last (citus-distributed-rule-from rule))))
               (column-definition
                (when last-from-rule
                  (find (column-name (citus-distributed-rule-using rule))
                        (table-field-list last-from-rule)
                        :test #'string=
                        :key #'column-name)))
               (new-column
                (when column-definition
                  (make-column :name (column-name column-definition)
                               :type-name (column-type-name column-definition)
                               :nullable (column-nullable column-definition)
                               :transform (column-transform column-definition)))))

          (if column-definition
              (progn
                ;;
                ;; Here also we need to add the new column to the PKEY
                ;; definition, in first position.
                ;;
                (add-column-to-pkey table (column-name new-column))

                ;;
                ;; We need to backfill the distribution key in the data,
                ;; which we're implementing with a JOIN when we SELECT from
                ;; the source table. We add the new field here.
                ;;
                (push new-column (table-field-list table))
                (push new-column (table-column-list table)))

              ;;
              ;; We don't have any table-field-list in the citus rule,
              ;; meaning that the distribute ... using ... clause is lacking
              ;; the FROM part, and we need it.
              ;;
              (error
               (make-condition 'citus-rule-is-missing-from-list :rule rule)))))))


(defun add-column-to-pkey (table column-name)
  "Add COLUMN in the first position of the TABLE's primary key index."
  (let* ((index  (find-if #'index-primary (table-index-list table)))
         (idxcol (when index
                   (find column-name (index-columns index) :test #'string=))))
    (when (and index (null idxcol))
      ;; add a new column
      (push column-name (index-columns index))
      ;; now remove origin schema sql and condef, we need to redo them
      (setf (index-sql index) nil)
      (setf (index-condef index) nil)

      ;; now tweak the fkey definitions that are using this index
      (loop :for fkey :in (index-fk-deps index)
         :do (push column-name (fkey-columns fkey))
         :do (push column-name (fkey-foreign-columns fkey))
         :do (setf (fkey-condef fkey) nil)))))


;;;
;;; Format a query for backfilling the data right from pgloader:
;;;
;;;   SELECT dist_key, * FROM source JOIN pivot ON ...
;;;
(defun format-citus-join-clause (table distribution-rule)
  "Format a JOIN clause to backfill the distribution key data in tables that
   are referencing (even indirectly) the main distribution table."
  (with-output-to-string (s)
    (loop :for current-table := table :then rel
       :for rel :in (citus-distributed-rule-from distribution-rule)
       :do (let* ((fkey
                   (find (ensure-unquoted (table-name rel))
                         (table-fkey-list current-table)
                         :test #'string=
                         :key (lambda (fkey)
                                (ensure-unquoted
                                 (table-name (fkey-foreign-table fkey))))))
                  (ftable (fkey-foreign-table fkey)))
             (format s
                     " JOIN ~s.~s"
                     (schema-source-name (table-schema ftable))
                     (table-source-name ftable))
             ;;
             ;; Skip the first column in the fkey definition, that's the
             ;; distribution key that was just added by pgloader: we don't
             ;; have it on the source database, we are going to create it on
             ;; the target database.
             ;;
             (loop :for first := t :then nil
                :for c :in (cdr (fkey-columns fkey))
                :for fc :in (cdr (fkey-foreign-columns fkey))
                :do (format s
                            " ~:[AND~;ON~] ~a.~a = ~a.~a"
                            first
                            (table-source-name (fkey-table fkey))
                            c
                            (table-source-name (fkey-foreign-table fkey))
                            fc))))))

(defun citus-format-sql-select (source-table target-table)
  "Return the SQL statement to use to fetch data from the COPY context,
   including backfilling the distribution key in related tables."

  ;;
  ;; SELECT from.id, id, ... from source join from-table ...
  ;;
  ;; So we must be careful to prefix the column names with the
  ;; proper table name, because of the join(s), and the first column
  ;; in the output is taken from the main FROM table (the last one
  ;; in the rule).
  ;;
  (let* ((last-from-rule
          (car (last (citus-distributed-rule-from
                      (table-citus-rule target-table)))))
         (cols
          (append (list
                   (format nil "~a.~a"
                           (table-name last-from-rule)
                           (column-name (first (table-field-list source-table)))))
                  (mapcar (lambda (field)
                            (format nil "~a.~a"
                                    (table-name source-table)
                                    (column-name field)))
                          (rest (table-field-list source-table)))))
         (joins
          (format-citus-join-clause source-table
                                    (table-citus-rule target-table))))
    (format nil
            "SELECT ~{~a::text~^, ~} FROM ~s.~s ~a"
            cols
            (schema-source-name (table-schema source-table))
            (table-source-name source-table)
            joins)))

;;;
;;; Predicate to see if a table needs backfilling
;;;
(defun citus-backfill-table-p (table)
  "Returns non-nil when given TABLE should be backfilled with the
   distribution key."
  (and (table-citus-rule table)
       (typep (table-citus-rule table) 'citus-distributed-rule)
       (not (null (citus-distributed-rule-from (table-citus-rule table))))))

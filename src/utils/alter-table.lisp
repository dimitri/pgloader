;;;
;;; ALTER TABLE allows pgloader to apply transformations on the catalog
;;; retrieved before applying it to PostgreSQL: SET SCHEMA, RENAME, etc.
;;;
(in-package :pgloader.catalog)

;;;
;;; Support for the INCLUDING and EXCLUDING clauses
;;;
(defstruct string-match-rule target)
(defstruct regex-match-rule target)

(defgeneric matches (rule string)
  (:documentation "Return non-nul if the STRING matches given RULE.")
  (:method ((rule string-match-rule) string)
    (string= (string-match-rule-target rule) string))

  (:method ((rule regex-match-rule) string)
    (cl-ppcre:scan (regex-match-rule-target rule) string)))

#|
   See src/parsers/command-alter-table.lisp

   (make-match-rule :type :regex
                    :target "_list$"
                    :action #'pgloader.schema::alter-table-set-schema
                    :args (list "mv"))
|#
(defstruct match-rule rule schema action args)

(defgeneric alter-table (object alter-table-rule-list))

(defmethod alter-table ((catalog catalog) alter-table-rule-list)
  "Apply ALTER-TABLE-RULE-LIST to all schema of CATALOG."
  (loop :for schema :in (catalog-schema-list catalog)
     :do (alter-table schema alter-table-rule-list)))

(defmethod alter-table ((schema schema) alter-table-rule-list)
  "Apply ALTER-TABLE-RULE-LIST to all tables and views of SCHEMA."
  (loop :for table :in (append (schema-table-list schema)
                               (schema-view-list schema))
     :do (alter-table table alter-table-rule-list)))

(defmethod alter-table ((table table) alter-table-rule-list)
  "Apply ALTER-TABLE-RULE-LIST to TABLE."
  ;;
  ;; alter-table-rule-list is a list of set of rules, within each set we
  ;; only apply the first rules that matches.
  ;;
  (loop :for rule-list :in alter-table-rule-list
     :do (let ((match-rule
                (loop :for match-rule :in rule-list
                   :thereis (when (rule-matches match-rule table)
                              match-rule))))
           (when match-rule
             (apply (match-rule-action match-rule)
                    (list* table (match-rule-args match-rule)))))))


;;;
;;; ALTER TABLE actions: functions that take a table and arguments and apply
;;; the altering action wanted to them.
;;;
(defun alter-table-set-schema (table schema-name)
  "Alter the schema of TABLE, set SCHEMA-NAME instead."
  (let* ((catalog (schema-catalog (table-schema table)))
         (schema  (maybe-add-schema catalog schema-name)))
    (setf (table-schema table) schema)

    ;; this voids any index definition extracted from the source database...
    (reset-sql-definitions table)))

(defun alter-table-rename (table new-name)
  "Alter the name of TABLE to NEW-NAME."
  (setf (table-name table) new-name))

(defun alter-table-set-storage-parameters (table parameters)
  "Alter the storage parameters of TABLE."
  (setf (table-storage-parameter-list table) parameters))

(defun alter-table-set-tablespace (table tablespace)
  "Alter the tablespace slot of TABLE"
  (setf (table-tablespace table) tablespace))


;;;
;;; Apply the match rules as given by the parser to a table name.
;;;

(defgeneric rule-matches (match-rule object)
  (:documentation "Returns non-nill when MATCH-RULE matches with OBJECT."))

(defmethod rule-matches ((match-rule match-rule) (table table))
  "Return non-nil when TABLE matches given MATCH-RULE."
  (let ((schema-name (schema-source-name (table-schema table)))
        (rule-schema (match-rule-schema match-rule))
        (table-name  (table-source-name table)))
    (when (or (null rule-schema)
              (and rule-schema (string= rule-schema schema-name)))
      (matches (match-rule-rule match-rule) table-name))))

(defmethod rule-matches ((match-rule match-rule) (schema schema))
  "Return non-nil when TABLE matches given MATCH-RULE."
  (let ((schema-name (schema-source-name schema)))
    (matches (match-rule-rule match-rule) schema-name)))


;;;
;;; Also implement ALTER SCHEMA support here, it's using the same underlying
;;; structure.
;;;
(defgeneric alter-schema (object alter-schema-rule-list))

(defmethod alter-schema ((catalog catalog) alter-schema-rule-list)
  "Apply ALTER-SCHEMA-RULE-LIST to all schema of CATALOG."
  (loop :for schema :in (catalog-schema-list catalog)
     :do (alter-schema schema alter-schema-rule-list)))

(defmethod alter-schema ((schema schema) alter-schema-rule-list)
  "Apply ALTER-SCHEMA-RULE-LIST to SCHEMA."
  ;;
  ;; alter-schema-rule-list is a list of set of rules, within each set we
  ;; only apply the first rules that matches.
  ;;
  (loop :for rule-list :in alter-schema-rule-list
     :do (let ((match-rule
                (loop :for match-rule :in rule-list
                   :thereis (when (rule-matches match-rule schema)
                              match-rule))))
           (when match-rule
             (apply (match-rule-action match-rule)
                    (list* schema (match-rule-args match-rule)))))))

(defun alter-schema-rename (schema new-name)
  "Alter the name fo the given schema to new-name."
  (setf (schema-name schema) new-name)

  ;; this voids any index definition extracted from the source database...
  (loop :for table :in (schema-table-list schema)
     :do (reset-sql-definitions table)))



;;;
;;; When a table targets a new schema, we need to refrain from using its
;;; index SQL definition when we got it from the source system, such as with
;;; pg_get_indexdef() on PostgreSQL.
;;;
(defun reset-sql-definitions (table)
  "Reset source database given wholesale SQL definition for table's indexes
   and foreign keys."
  (loop :for index :in (table-index-list table)
     :do (when (index-sql index)
           (setf (index-sql index) nil)))

  (loop :for fkey :in (table-fkey-list table)
     :do (when (fkey-condef fkey)
           (setf (fkey-condef fkey) nil))))

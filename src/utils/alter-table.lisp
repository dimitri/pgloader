;;;
;;; ALTER TABLE allows pgloader to apply transformations on the catalog
;;; retrieved before applying it to PostgreSQL: SET SCHEMA, RENAME, etc.
;;;
(in-package :pgloader.schema)

#|
   See src/parsers/command-alter-table.lisp

   (make-match-rule :type :regex
                    :target "_list$"
                    :action #'pgloader.schema::alter-table-set-schema
                    :args (list "mv"))
|#
(defstruct match-rule type target action args)

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
    (setf (table-schema table) schema)))

(defun alter-table-rename (table new-name)
  "Alter the name of TABLE to NEW-NAME."
  (setf (table-name table) new-name))


;;;
;;; Apply the match rules as given by the parser to a table name.
;;;

(defgeneric rule-matches (match-rule object)
  (:documentation "Returns non-nill when MATCH-RULE matches with OBJECT."))

(defmethod rule-matches ((match-rule match-rule) (table table))
  "Return non-nil when TABLE matches given MATCH-RULE."
  (let ((table-name (table-source-name table)))
    (ecase (match-rule-type match-rule)
      (:string (string= (match-rule-target match-rule) table-name))
      (:regex  (cl-ppcre:scan (match-rule-target match-rule) table-name)))))

(defmethod rule-matches ((match-rule match-rule) (schema schema))
  "Return non-nil when TABLE matches given MATCH-RULE."
  (let ((schema-name (schema-source-name schema)))
    (ecase (match-rule-type match-rule)
      (:string (string= (match-rule-target match-rule) schema-name))
      (:regex  (cl-ppcre:scan (match-rule-target match-rule) schema-name)))))


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
  (setf (schema-name schema) new-name))

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
  (setf (table-schema table) schema-name))

(defun alter-table-rename (table new-name)
  "Alter the name of TABLE to NEW-NAME."
  (setf (table-name table) new-name))


;;;
;;; Apply the match rules as given by the parser to a table name.
;;;

(declaim (inline rule-matches))
(defun rule-matches (match-rule table)
  "Return non-nil when TABLE matches given MATCH-RULE."
  (declare (type match-rule match-rule) (type table table))
  (let ((table-name (table-source-name table)))
    (ecase (match-rule-type match-rule)
      (:string (string= (match-rule-target match-rule) table-name))
      (:regex  (cl-ppcre:scan (match-rule-target match-rule) table-name)))))


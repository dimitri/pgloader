;;;
;;; pgloader supports several PostgreSQL variants:
;;;
;;;   :pgdg      Core PostgreSQL from the PostgreSQL Development Group
;;;   :redshift  The Amazon Redshift variant
;;;
;;; Some variants have less features than others, for instance Redshift has
;;; a very limited set of data types to choose from. We have to dumb-down
;;; the PostgreSQL catalogs to target such a dumb database technology.
;;;
;;; Still, Redshift supports the same protocol as PostgreSQL, including
;;; COPY, so it's nice to consider this target as a PostgreSQL thing
;;; nonetheless.

(in-package #:pgloader.pgsql)

(defun finalize-catalogs (catalog variant)
  ;;
  ;; For Core PostgreSQL, we also want to find data types names that have
  ;; no Btree support and fetch alternatives. This allows for supporting
  ;; automated migration of geometric data types.
  ;;
  (when (eq :pgdg variant)
    (setf (catalog-types-without-btree catalog)
          (list-typenames-without-btree-support)))

  ;;
  ;; For other variants, we might have to be smart about the selection of
  ;; data types…
  ;;
  (adjust-data-types catalog variant))

(defgeneric adjust-data-types (catalog variant))

;;;
;;; Nothing needs to be done for PostgreSQL variant :pgdg, of course.
;;;
(defmethod adjust-data-types ((catalog catalog) (variant (eql :pgdg)))
  catalog)

;;;
;;; The RedShift case is a little more involved, as shown in their
;;; documentation:
;;;
;;; https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
;;;
;;; Redshift only support those data types:
;;;

(defvar *redshift-supported-data-types*
  '("SMALLINT"
    "INTEGER"
    "BIGINT"
    "DECIMAL"
    "REAL"
    "DOUBLE PRECISION"
    "BOOLEAN"
    "CHAR"
    "VARCHAR"
    "DATE"
    "TIMESTAMP"
    "TIMESTAMPTZ"))

(defvar *redshift-decimal-max-precision* 38)

(defmethod adjust-data-types ((catalog catalog) (variant (eql :redshift)))
  (dumb-down-data-types-for-redshift catalog))

;;;
;;; Catalog data types walker
;;;
(defgeneric dumb-down-data-types-for-redshift (object))

(defmethod dumb-down-data-types-for-redshift ((catalog catalog))
  (loop :for schema :in (catalog-schema-list catalog)
     :do (dumb-down-data-types-for-redshift schema)))

(defmethod dumb-down-data-types-for-redshift ((schema schema))
  (loop :for table :in (append (schema-table-list schema)
                               (schema-view-list schema))
     :do (dumb-down-data-types-for-redshift table)))

(defmethod dumb-down-data-types-for-redshift ((table table))
  (loop :for column :in (table-column-list table)
     :do (dumb-down-data-types-for-redshift column)))

(defmethod dumb-down-data-types-for-redshift ((column column))
  (cond ((and (stringp (column-type-name column))
              (member (column-type-name column)
                      *redshift-supported-data-types*
                      :test #'string-equal))
         ;; the target data type is supported... but we might have other
         ;; situations to deal with, such as
         ;;
         ;; DECIMAL precision 65 must be between 1 and 38
         (when (and (member (column-type-name column)
                            '("decimal" "numeric")
                            :test #'string-equal)
                    (column-type-mod column))
           (destructuring-bind (precision &optional (scale 0))
               (parse-column-typemod (column-type-name column)
                                     (column-type-mod column))
             (when (< *redshift-decimal-max-precision* precision)
               (log-message :info
                            "Redshift doesn't support DECIMAL(~a), capping to ~a"
                            precision *redshift-decimal-max-precision*)
               ;;
               ;; Internal pgloader API want the value as a string.
               ;;
               (setf (column-type-mod column)
                     (format nil "(~a,~a)"
                             *redshift-decimal-max-precision* scale))))))

        (t
         (let ((target-data-type "varchar")
               (target-type-mod  "(512)"))
           ;;
           ;; TODO: we might want to be smarter about the data type conversion
           ;; here, or at least a tad less dumb. For instance:
           ;;
           ;;   - when the source column is an ENUM, we know from its source
           ;;     definition the maximum length of the target column
           ;;
           ;;   - when the source field is e.g. a MySQL varchar(12), it might be
           ;;   - nice to keep the typemod here, or maybe 4 times the typemod to
           ;;   - take into account Redshift encoding "properties".
           ;;
           (setf (column-type-name column) target-data-type)
           (setf (column-type-mod column)  target-type-mod)

           (log-message :info
                        "Redshift support: ~a change type from ~a to ~a~a"
                        (column-name column)
                        (column-type-name column)
                        target-data-type
                        target-type-mod)))))

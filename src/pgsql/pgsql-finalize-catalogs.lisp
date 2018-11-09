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
  "Finalize the target PostgreSQL catalogs, dumbing down datatypes when the
   target actually is Redshift rather than core PostgreSQL."
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

(defgeneric adjust-data-types (catalog variant)
  (:documentation
   "Adjust PostgreSQL data types depending on the variant we target."))

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
(defvar *redshift-varchar-max-precision* 65535)

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
  (let ((target-data-type "varchar")
        (target-type-mod  "(512)"))
    (cond (
           ;;
           ;; when the source column is an ENUM, we know from its source
           ;; definition the maximum length of the target column
           ;;
           (and (typep (column-type-name column) 'sqltype)
                (typep (sqltype-extra (column-type-name column)) 'list))

           (let ((enum-value-list
                  (sqltype-extra (column-type-name column))))
             (setf target-data-type "varchar")
             (setf target-type-mod
                   (format nil
                           "(~a)"
                           ;; times 3 because Redshift doesn't know
                           ;; how to count chars in unicode, it only
                           ;; knows how to count bytes
                           (* 3
                              (reduce #'max
                                      enum-value-list
                                      :key #'length
                                      :initial-value 0))))))

          ;;
          ;; When using "text" (unbounded varchar) in MySQL, set the
          ;; precision of the varchar to the max of what Redshift can
          ;; handle.
          ;;
          ((string-equal (column-type-name column) "text")
           (setf target-data-type "varchar")
           (setf target-type-mod
                 (format nil "(~a)" *redshift-varchar-max-precision*)))

          ;;
          ;; when the source field is e.g. a MySQL varchar(12), we can keep
          ;; the typemod here, or maybe 3 times the typemod to take into
          ;; account Redshift encoding "properties".
          ;;
          ((string-equal (column-type-name column) "varchar")
           (destructuring-bind (precision &optional (scale 0))
               (parse-column-typemod (column-type-name column)
                                     (column-type-mod column))
             (declare (ignore scale))
             ;; Redshift can't count chars, it counts bytes, and we
             ;; are dealing with utf-8.
             (setf target-type-mod
                   (format nil "(~a)"
                           (min (* 3 precision)
                                *redshift-varchar-max-precision*)))))

          ;;
          ;; Redshift has a special limit with decimal and numeric data
          ;; types, so we must handle that specifically here:
          ;;
          ;;   DECIMAL precision 65 must be between 1 and 38
          ;;
          ((and (member (column-type-name column)
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
               (setf target-data-type "numeric")
               (setf target-type-mod
                     (format nil "(~a,~a)"
                             *redshift-decimal-max-precision* scale)))))

          ;;
          ;; Target data type is suppported, just keep it around.
          ;;
          ((and (stringp (column-type-name column))
                (member (column-type-name column)
                        *redshift-supported-data-types*
                        :test #'string-equal))
           (setf target-data-type (column-type-name column))
           (setf target-type-mod  (column-type-mod column))))

    (setf (column-type-name column) target-data-type)
    (setf (column-type-mod column)  target-type-mod)

    (log-message :info
                 "Redshift support: ~a change type from ~a to ~a~a"
                 (column-name column)
                 (column-type-name column)
                 target-data-type
                 target-type-mod)))

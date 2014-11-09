;;;
;;; Tools to handle the SQLite Database
;;;

(in-package :pgloader.sqlite)

(defvar *sqlite-db* nil
  "The SQLite database connection handler.")

(defparameter *sqlite-default-cast-rules*
  `((:source (:type "character") :target (:type "text" :drop-typemod t))
    (:source (:type "varchar")   :target (:type "text" :drop-typemod t))
    (:source (:type "nvarchar")  :target (:type "text" :drop-typemod t))
    (:source (:type "char")      :target (:type "text" :drop-typemod t))
    (:source (:type "nchar")     :target (:type "text" :drop-typemod t))
    (:source (:type "clob")      :target (:type "text" :drop-typemod t))

    (:source (:type "tinyint") :target (:type "smallint"))

    (:source (:type "float") :target (:type "float")
             :using pgloader.transforms::float-to-string)

    (:source (:type "real") :target (:type "real")
             :using pgloader.transforms::float-to-string)

    (:source (:type "double") :target (:type "double precision")
             :using pgloader.transforms::float-to-string)

    (:source (:type "numeric") :target (:type "numeric")
             :using pgloader.transforms::float-to-string)

    (:source (:type "blob") :target (:type "bytea")
             :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "datetime") :target (:type "timestamptz")
             :using pgloader.transforms::sqlite-timestamp-to-timestamp)

    (:source (:type "timestamp") :target (:type "timestamp")
             :using pgloader.transforms::sqlite-timestamp-to-timestamp)

    (:source (:type "timestamptz") :target (:type "timestamptz")
             :using pgloader.transforms::sqlite-timestamp-to-timestamp))
  "Data Type Casting to migrate from SQLite to PostgreSQL")

(defstruct (coldef
	     (:constructor make-coldef (table-name
                                        seq name dtype ctype
                                        nullable default pk-id)))
  table-name seq name dtype ctype nullable default pk-id)

(defun normalize (sqlite-type-name)
  "SQLite only has a notion of what MySQL calls column_type, or ctype in the
   CAST machinery. Transform it to the data_type, or dtype."
  (let* ((sqlite-type-name (string-downcase sqlite-type-name))
         (tokens (remove-if (lambda (token)
                              (member token '("unsigned" "short"
                                              "varying" "native")
                                      :test #'string-equal))
                            (sq:split-sequence #\Space sqlite-type-name))))
    (assert (= 1 (length tokens)))
    (first tokens)))

(defun ctype-to-dtype (sqlite-type-name)
  "In SQLite we only get the ctype, e.g. int(7), but here we want the base
   data type behind it, e.g. int."
  (let* ((ctype     (normalize sqlite-type-name))
         (paren-pos (position #\( ctype)))
    (if paren-pos (subseq ctype 0 paren-pos) ctype)))

(defun cast-sqlite-column-definition-to-pgsql (sqlite-column)
  "Return the PostgreSQL column definition from the MySQL one."
  (multiple-value-bind (column fn)
      (with-slots (table-name name dtype ctype default nullable)
          sqlite-column
        (cast table-name name dtype ctype default nullable nil))
    ;; the SQLite driver smartly maps data to the proper CL type, but the
    ;; pgloader API only wants to see text representations to send down the
    ;; COPY protocol.
    (values column (or fn (lambda (val) (if val (format nil "~a" val) :null))))))

(defmethod cast-to-bytea-p ((col coldef))
  "Returns a generalized boolean, non-nil when the column is casted to a
   PostgreSQL bytea column."
  (string= "bytea" (cast-sqlite-column-definition-to-pgsql col)))

(defmethod format-pgsql-column ((col coldef) &key identifier-case)
  "Return a string representing the PostgreSQL column definition."
  (let* ((column-name
	  (apply-identifier-case (coldef-name col) identifier-case))
	 (type-definition
          (with-slots (table-name name dtype ctype nullable default)
              col
            (cast table-name name dtype ctype default nullable nil))))
    (format nil "~a ~22t ~a" column-name type-definition)))

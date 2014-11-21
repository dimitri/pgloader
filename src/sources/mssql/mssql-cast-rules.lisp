;;;
;;; Tools to handle MS SQL data type casting rules
;;;

(in-package :pgloader.mssql)

(defparameter *mssql-default-cast-rules*
  `((:source (:type "char")      :target (:type "text" :drop-typemod t))
    (:source (:type "nchar")     :target (:type "text" :drop-typemod t))
    (:source (:type "varchar")   :target (:type "text" :drop-typemod t))
    (:source (:type "nvarchar")  :target (:type "text" :drop-typemod t))
    (:source (:type "xml")       :target (:type "text" :drop-typemod t))

    (:source (:type "bit") :target (:type "boolean"))

    (:source (:type "uniqueidentifier") :target (:type "uuid")
             :using pgloader.transforms::sql-server-uniqueidentifier-to-uuid)

    (:source (:type "hierarchyid") :target (:type "bytea")
             :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "geography") :target (:type "bytea")
         :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "tinyint") :target (:type "smallint"))

    (:source (:type "float") :target (:type "float")
             :using pgloader.transforms::float-to-string)

    (:source (:type "real") :target (:type "real")
             :using pgloader.transforms::float-to-string)

    (:source (:type "double") :target (:type "double precision")
             :using pgloader.transforms::float-to-string)

    (:source (:type "numeric") :target (:type "numeric")
             :using pgloader.transforms::float-to-string)

    (:source (:type "money") :target (:type "numeric")
             :using pgloader.transforms::float-to-string)

    (:source (:type "smallmoney") :target (:type "numeric")
             :using pgloader.transforms::float-to-string)

    (:source (:type "binary") :target (:type "bytea")
             :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "varbinary") :target (:type "bytea")
             :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "datetime") :target (:type "timestamptz"))
    (:source (:type "datetime2") :target (:type "timestamptz")))
  "Data Type Casting to migrate from MSSQL to PostgreSQL")

;;;
;;; Specific implementation of schema migration, see the API in
;;; src/pgsql/schema.lisp
;;;
(defstruct (mssql-column
	     (:constructor make-mssql-column
			   (schema table-name name type
                                   default nullable identity
                                   character-maximum-length
                                   numeric-precision
                                   numeric-precision-radix
                                   numeric-scale
                                   datetime-precision
                                   character-set-name
                                   collation-name)))
  schema table-name name type default nullable identity
  character-maximum-length
  numeric-precision numeric-precision-radix numeric-scale
  datetime-precision
  character-set-name collation-name)

(defmethod mssql-column-ctype ((col mssql-column))
  "Build the ctype definition from the full mssql-column information."
  (let ((type (mssql-column-type col)))
    (cond ((and (string= type "int")
                (mssql-column-identity col))
           "bigserial")

          ((member type
                   '("decimal" "numeric" "float" "double" "real")
                   :test #'string=)
           (format nil "~a(~a,~a)"
                   type
                   (mssql-column-numeric-precision col)
                   (mssql-column-numeric-scale col)))

          (t type))))

(defmethod format-pgsql-column ((col mssql-column))
  "Return a string representing the PostgreSQL column definition."
  (let* ((column-name (apply-identifier-case (mssql-column-name col)))
	 (type-definition
	  (with-slots (schema table-name name type default nullable)
	      col
            (declare (ignore schema))   ; FIXME
            (let ((ctype (mssql-column-ctype col)))
              (cast table-name name type ctype default nullable nil)))))
    (format nil "~a ~22t ~a" column-name type-definition)))

(defun cast-mssql-column-definition-to-pgsql (mssql-column)
  "Return the PostgreSQL column definition from the MS SQL one."
  (multiple-value-bind (column fn)
      (with-slots (schema table-name name type default nullable)
          mssql-column
        (declare (ignore schema))       ; FIXME
        (let ((ctype (mssql-column-ctype mssql-column)))
          (cast table-name name type ctype default nullable nil)))

    ;; the MS SQL driver smartly maps data to the proper CL type, but the
    ;; pgloader API only wants to see text representations to send down the
    ;; COPY protocol.
    (values column (or fn (lambda (val) (if val (format nil "~a" val) :null))))))

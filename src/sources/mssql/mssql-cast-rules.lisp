;;;
;;; Tools to handle MS SQL data type casting rules
;;;

(in-package :pgloader.source.mssql)

(defparameter *mssql-default-cast-rules*
  `((:source (:type "char")      :target (:type "text" :drop-typemod t))
    (:source (:type "nchar")     :target (:type "text" :drop-typemod t))
    (:source (:type "varchar")   :target (:type "text" :drop-typemod t))
    (:source (:type "nvarchar")  :target (:type "text" :drop-typemod t))
    (:source (:type "ntext")     :target (:type "text" :drop-typemod t))
    (:source (:type "xml")       :target (:type "xml" :drop-typemod t))

    (:source (:type "int" :auto-increment t)
             :target (:type "bigserial" :drop-default t))
    
    (:source (:type "bigint" :auto-increment t)
             :target (:type "bigserial"))

    (:source (:type "smallint" :auto-increment t)
             :target (:type "smallserial"))

    (:source (:type "tinyint") :target (:type "smallint"))

    (:source (:type "tinyint" :auto-increment t)
             :target (:type "serial"))

    (:source (:type "bit") :target (:type "boolean")
             :using pgloader.transforms::sql-server-bit-to-boolean)

    (:source (:type "uniqueidentifier") :target (:type "uuid")
             :using pgloader.transforms::sql-server-uniqueidentifier-to-uuid)

    (:source (:type "hierarchyid") :target (:type "bytea")
             :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "geography") :target (:type "bytea")
         :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "float") :target (:type "float")
             :using pgloader.transforms::float-to-string)

    (:source (:type "real") :target (:type "real")
             :using pgloader.transforms::float-to-string)

    (:source (:type "double") :target (:type "double precision")
             :using pgloader.transforms::float-to-string)

    (:source (:type "numeric") :target (:type "numeric")
             :using pgloader.transforms::float-to-string)

    (:source (:type "decimal") :target (:type "numeric")
             :using pgloader.transforms::float-to-string)

    (:source (:type "money") :target (:type "numeric")
             :using pgloader.transforms::float-to-string)

    (:source (:type "smallmoney") :target (:type "numeric")
             :using pgloader.transforms::float-to-string)

    (:source (:type "binary") :target (:type "bytea")
             :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "image") :target (:type "bytea")
             :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "varbinary") :target (:type "bytea")
             :using pgloader.transforms::byte-vector-to-bytea)

    (:source (:type "smalldatetime") :target (:type "timestamptz"))
    (:source (:type "datetime") :target (:type "timestamptz"))
    (:source (:type "datetime2") :target (:type "timestamptz"))
    (:source (:type "datetimeoffset") :target (:type "timestamptz")))
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

(defmethod field-name ((field mssql-column) &key)
  (mssql-column-name field))

(defmethod mssql-column-ctype ((col mssql-column))
  "Build the ctype definition from the full mssql-column information."
  (let ((type (mssql-column-type col)))
    (cond ((member type '("float" "real") :test #'string=)
           ;; see https://msdn.microsoft.com/en-us/library/ms173773.aspx
           ;; scale is supposed to be nil, and useless in PostgreSQL, so we
           ;; just ignore it
           (format nil "~a(~a)" type (mssql-column-numeric-precision col)))

          ((member type '("decimal" "numeric" ) :test #'string=)
           ;; https://msdn.microsoft.com/en-us/library/ms187746.aspx
           (cond ((null (mssql-column-numeric-precision col))
                  type)
                 (t
                  (format nil "~a(~a,~a)"
                          type
                          (mssql-column-numeric-precision col)
                          (or (mssql-column-numeric-scale col) 0)))))

          ((member type '("char" "nchar" "varchar" "nvarchar" "binary")
                   :test #'string=)
           ;; the user might have a CAST rule with keep typemod, so we need
           ;; to deal with character-maximum-length for now

           (if (= -1 (mssql-column-character-maximum-length col))
               type
               (format nil "~a(~a)"
                       type (mssql-column-character-maximum-length col))))

          (t type))))

(defmethod cast ((field mssql-column) &key &allow-other-keys)
  "Return the PostgreSQL type definition from given MS SQL column definition."
  (with-slots (schema table-name name type default nullable)
      field
    (declare (ignore schema))           ; FIXME
    (let* ((ctype (mssql-column-ctype field))
           (extra (when (mssql-column-identity field) :auto-increment))
           (pgcol
            (apply-casting-rules table-name name type ctype default nullable extra)))
      ;; the MS SQL driver smartly maps data to the proper CL type, but the
      ;; pgloader API only wants to see text representations to send down the
      ;; COPY protocol.
      (unless (column-transform pgcol)
        (setf (column-transform pgcol)
              (lambda (val) (if val (format nil "~a" val) :null))))

      ;; normalize default values
      ;; see pgloader.psql:*pgsql-default-values*
      (let ((default (column-default pgcol)))
        (setf (column-default pgcol)
              (cond
                ((and (null default) (column-nullable pgcol))
                 :null)

                ((and (stringp default) (string= "NULL" default))
                 :null)

                ;; fix stupid N'' behavior from MS SQL column default
                ((and (stringp default)
                      (uiop:string-enclosed-p "N'" default "'"))
                 (subseq default 2 (+ (length default) -1)))

                ((and (stringp default)
                      ;; address CURRENT_TIMESTAMP(6) and other spellings
                      (or (uiop:string-prefix-p "CURRENT_TIMESTAMP" default)
                          (string= "CURRENT TIMESTAMP" default)))
                 :current-timestamp)

                ((and (stringp default)
                      (or (string= "newid()" default)
                          (string= "newsequentialid()" default)
                          (string= "GENERATE_UUID" default)))
                 :generate-uuid)

                (t (column-default pgcol)))))
      pgcol)))


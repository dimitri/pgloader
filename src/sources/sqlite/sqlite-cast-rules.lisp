;;;
;;; Tools to handle the SQLite Database
;;;

(in-package :pgloader.source.sqlite)

(defvar *sqlite-db* nil
  "The SQLite database connection handler.")

(defparameter *sqlite-default-cast-rules*
  `((:source (:type "character") :target (:type "text" :drop-typemod t))
    (:source (:type "varchar")   :target (:type "text" :drop-typemod t))
    (:source (:type "nvarchar")  :target (:type "text" :drop-typemod t))
    (:source (:type "char")      :target (:type "text" :drop-typemod t))
    (:source (:type "nchar")     :target (:type "text" :drop-typemod t))
    (:source (:type "clob")      :target (:type "text" :drop-typemod t))

    (:source (:type "integer" :auto-increment t) :target (:type "bigserial"))

    (:source (:type "tinyint") :target (:type "smallint")
             :using pgloader.transforms::integer-to-string)

    (:source (:type "integer") :target (:type "bigint")
             :using pgloader.transforms::integer-to-string)

    (:source (:type "long") :target (:type "bigint")
             :using pgloader.transforms::integer-to-string)

    (:source (:type "float") :target (:type "float")
             :using pgloader.transforms::float-to-string)

    (:source (:type "real") :target (:type "real")
             :using pgloader.transforms::float-to-string)

    (:source (:type "double") :target (:type "double precision")
             :using pgloader.transforms::float-to-string)

    (:source (:type "double precision") :target (:type "double precision")
             :using pgloader.transforms::float-to-string)

    (:source (:type "numeric") :target (:type "numeric" :drop-typemod nil)
             :using pgloader.transforms::float-to-string)

    (:source (:type "decimal") :target (:type "decimal" :drop-typemod nil)
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
  table-name seq name dtype ctype nullable default pk-id extra)

(defun normalize (sqlite-type-name)
  "SQLite only has a notion of what MySQL calls column_type, or ctype in the
   CAST machinery. Transform it to the data_type, or dtype."
  (multiple-value-bind (type-name typmod extra-noise-words)
      (pgloader.parser:parse-sqlite-type-name sqlite-type-name)
    (declare (ignore extra-noise-words))
    (if typmod
        (format nil "~a(~a~@[,~a~])" type-name (car typmod) (cdr typmod))
        type-name)))

(defun ctype-to-dtype (sqlite-type-name)
  "In SQLite we only get the ctype, e.g. int(7), but here we want the base
   data type behind it, e.g. int."
  ;; parse-sqlite-type-name returns multiple values, here we only need the
  ;; first one: (type-name typmod extra-noise-words)
  (pgloader.parser:parse-sqlite-type-name sqlite-type-name))

(defmethod cast ((col coldef) &key &allow-other-keys)
  "Return the PostgreSQL type definition from given SQLite column definition."
  (with-slots (table-name name dtype ctype default nullable extra)
      col
    (let ((pgcol
           (apply-casting-rules table-name name dtype ctype default nullable extra)))
      ;; the SQLite driver smartly maps data to the proper CL type, but the
      ;; pgloader API only wants to see text representations to send down
      ;; the COPY protocol.
      (unless (column-transform pgcol)
        (setf (column-transform pgcol)
              (lambda (val) (if val (format nil "~a" val) :null))))

      ;; normalize default values that comes from the casting rules
      ;; (respecting user cast decision to "drop default" or "keep default")
      (let ((default (column-default pgcol)))
        (setf (column-default pgcol)
              (cond
                ((and (stringp default) (string= "NULL" default))
                 :null)

                ((and (stringp default)
                      ;; address CURRENT_TIMESTAMP(6) and other spellings
                      (or (uiop:string-prefix-p "CURRENT_TIMESTAMP" default)
                          (string= "CURRENT TIMESTAMP" default)))
                 :current-timestamp)

                ((and (stringp default)
                      ;; we don't care about spaces in that expression
                      (string-equal "datetime('now','localtime')"
                                    (remove #\Space default)))
                 :current-timestamp)

                ((and (stringp default) (string-equal "current_date" default))
                 :current-date)

                ((stringp default)
                 ;; at least quote the single quotes in there
                 (cl-ppcre:regex-replace-all "[']" default "''"))

                (t (column-default pgcol)))))

      pgcol)))


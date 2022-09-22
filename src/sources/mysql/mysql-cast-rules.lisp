;;;
;;; Tools to handle MySQL data type casting rules
;;;

(in-package :pgloader.source.mysql)

(defun enum-or-set-name (table-name column-name type ctype typemod)
  (declare (ignore type ctype typemod))
  (apply-identifier-case
   (format nil "~a_~a" (unquote table-name #\") (unquote column-name #\"))))

;;;
;;; The default MySQL Type Casting Rules
;;;
(defparameter *mysql-default-cast-rules*
  `((:source (:type "int" :auto-increment t :typemod (< precision 10))
     :target (:type "serial"))

    (:source (:type "int" :auto-increment t :typemod (<= 10 precision))
     :target (:type "bigserial"))

    (:source (:type "int" :auto-increment nil :typemod (< precision 10))
     :target (:type "int"))

    (:source (:type "int" :auto-increment nil :typemod (<= 10 precision))
     :target (:type "bigint"))

    ;; bigint mediumint smallint and tinyint with auto_increment always are [big]serial
    (:source (:type "tinyint" :auto-increment t) :target (:type "serial"))
    (:source (:type "smallint" :auto-increment t) :target (:type "serial"))
    (:source (:type "mediumint" :auto-increment t) :target (:type "serial"))
    (:source (:type "bigint" :auto-increment t) :target (:type "bigserial"))

    ;; actually tinyint(1) is most often used as a boolean
    (:source (:type "tinyint" :typemod (= 1 precision))
	     :target (:type "boolean" :drop-typemod t)
	     :using pgloader.transforms::tinyint-to-boolean)

    ;; bit(1) is most often used as a boolean too
    (:source (:type "bit" :typemod (= 1 precision))
	     :target (:type "boolean" :drop-typemod t)
	     :using pgloader.transforms::bits-to-boolean)

    ;; bit(X) might be flags or another use case for bitstrings
    (:source (:type "bit")
	     :target (:type "bit" :drop-typemod nil)
	     :using pgloader.transforms::bits-to-hex-bitstring)

    ;; bigint(20) signed do fit into PostgreSQL bigint
    ;; (-9223372036854775808 to +9223372036854775807):
    (:source (:type "bigint" :signed t)
             :target (:type "bigint" :drop-typemod t))

    ;; bigint(20) unsigned does not fit into PostgreSQL bigint
    (:source (:type "bigint" :typemod (< 19 precision))
             :target (:type "numeric" :drop-typemod t))

    ;; now unsigned types
    (:source (:type "tinyint" :unsigned t)
             :target (:type "smallint" :drop-typemod t))
    (:source (:type "smallint" :unsigned t)
             :target (:type "integer" :drop-typemod t))
    (:source (:type "mediumint" :unsigned t)
             :target (:type "integer"  :drop-typemod t))
    (:source (:type "integer" :unsigned t)
             :target (:type "bigint"  :drop-typemod t))
    (:source (:type "int" :unsigned t)
             :target (:type "bigint"  :drop-typemod t))

    (:source (:type "int" :unsigned t :auto-increment t)
              :target (:type "bigserial" :drop-typemod t))
    (:source (:type "int" :signed t :auto-increment t)
              :target (:type "serial" :drop-typemod t))

    ;; we need the following to benefit from :drop-typemod
    (:source (:type "tinyint")   :target (:type "smallint" :drop-typemod t))
    (:source (:type "smallint")  :target (:type "smallint" :drop-typemod t))
    (:source (:type "mediumint") :target (:type "integer"  :drop-typemod t))
    (:source (:type "integer")   :target (:type "integer"  :drop-typemod t))
    (:source (:type "float")     :target (:type "float"    :drop-typemod t))
    (:source (:type "bigint")    :target (:type "bigint"   :drop-typemod t))

    (:source (:type "double")
	     :target (:type "double precision" :drop-typemod t))

    (:source (:type "numeric")
     :target (:type "numeric" :drop-typemod nil))

    (:source (:type "decimal")
     :target (:type "decimal" :drop-typemod nil))

    ;; the text based types
    (:source (:type "tinytext")   :target (:type "text") :using pgloader.transforms::remove-null-characters)
    (:source (:type "text")       :target (:type "text") :using pgloader.transforms::remove-null-characters)
    (:source (:type "mediumtext") :target (:type "text") :using pgloader.transforms::remove-null-characters)
    (:source (:type "longtext")   :target (:type "text") :using pgloader.transforms::remove-null-characters)

    (:source (:type "varchar")
     :target (:type "varchar" :drop-typemod nil)
     :using pgloader.transforms::remove-null-characters)

    (:source (:type "char")
     :target (:type "char" :drop-typemod nil)
     :using pgloader.transforms::remove-null-characters)

    ;;
    ;; cl-mysql returns binary values as a simple-array of bytes (as in
    ;; '(UNSIGNED-BYTE 8)), that we then need to represent as proper
    ;; PostgreSQL bytea input.
    ;;
    (:source (:type "binary") :target (:type "bytea")
	     :using pgloader.transforms::byte-vector-to-bytea)
    (:source (:type "varbinary")  :target (:type "bytea")
	     :using pgloader.transforms::byte-vector-to-bytea)
    (:source (:type "tinyblob")   :target (:type "bytea")
	     :using pgloader.transforms::byte-vector-to-bytea)
    (:source (:type "blob")       :target (:type "bytea")
	     :using pgloader.transforms::byte-vector-to-bytea)
    (:source (:type "mediumblob") :target (:type "bytea")
	     :using pgloader.transforms::byte-vector-to-bytea)
    (:source (:type "longblob")   :target (:type "bytea")
	     :using pgloader.transforms::byte-vector-to-bytea)

    ;;
    ;; MySQL and dates...
    ;;
    (:source (:type "datetime" :default "0000-00-00 00:00:00" :not-null t)
     :target (:type "timestamptz" :drop-default t :drop-not-null t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "datetime" :default "0000-00-00 00:00:00")
     :target (:type "timestamptz" :drop-default t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "datetime" :on-update-current-timestamp t :not-null t)
     :target (:type "timestamptz" :drop-default t :drop-not-null t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "datetime" :on-update-current-timestamp t :not-null nil)
     :target (:type "timestamptz" :drop-default t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "timestamp" :default "0000-00-00 00:00:00" :not-null t)
     :target (:type "timestamptz" :drop-default t :drop-not-null t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "timestamp" :default "0000-00-00 00:00:00")
     :target (:type "timestamptz" :drop-default t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "timestamp" :on-update-current-timestamp t :not-null t)
     :target (:type "timestamptz" :drop-default t :drop-not-null t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "timestamp" :on-update-current-timestamp t :not-null nil)
     :target (:type "timestamptz" :drop-default t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "date" :default "0000-00-00")
     :target (:type "date" :drop-default t)
     :using pgloader.transforms::zero-dates-to-null)

    ;; date types without strange defaults
    (:source (:type "date")      :target (:type "date"))
    (:source (:type "year")      :target (:type "integer" :drop-typemod t))

    (:source (:type "datetime")
     :target (:type "timestamptz")
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "timestamp")
     :target (:type "timestamptz")
     :using pgloader.transforms::zero-dates-to-null)

    ;; Inline MySQL "interesting" datatype
    (:source (:type "enum")
     :target (:type ,#'enum-or-set-name))

    (:source (:type "set")
     :target (:type ,#'enum-or-set-name)
     :using pgloader.transforms::set-to-enum-array)

    ;; geometric data types, just POINT for now
    (:source (:type "geometry")
     :target (:type "point")
     :using pgloader.transforms::convert-mysql-point)

    (:source (:type "point")
     :target (:type "point")
     :using pgloader.transforms::convert-mysql-point)

    (:source (:type "linestring")
     :target (:type "path")
     :using pgloader.transforms::convert-mysql-linestring))
  "Data Type Casting rules to migrate from MySQL to PostgreSQL")


;;;
;;; MySQL specific fields representation and low-level
;;;
(defstruct (mysql-column
	     (:constructor make-mysql-column
			   (table-name name comment dtype ctype default nullable extra)))
  table-name name dtype ctype default nullable extra comment)

(defmethod field-name ((field mysql-column) &key)
  (mysql-column-name field))

(defun explode-mysql-enum (ctype)
  "Convert MySQL ENUM expression into a list of labels."
  (cl-ppcre:register-groups-bind (list)
      ("(?i)(?:ENUM|SET)\\s*\\((.*)\\)" ctype)
    (first (cl-csv:read-csv list :separator #\, :quote #\' :escape "''"))))

(defun normalize-extra (extra)
  "Normalize MySQL strings into pgloader CL keywords for internal processing."
  (cond ((string= "auto_increment" extra)
         :auto-increment)

        ((or (string= extra "on update CURRENT_TIMESTAMP")
             (string= extra "on update current_timestamp()"))
         :on-update-current-timestamp)))

(defmethod cast ((col mysql-column) &key table)
  "Return the PostgreSQL type definition from given MySQL column definition."
  (with-slots (table-name name dtype ctype default nullable extra comment)
      col
    (let* ((pgcol
            (apply-casting-rules table-name name dtype ctype default nullable extra)))
      (setf (column-comment pgcol) comment)

      ;; normalize default values that are left after applying user defined
      ;; casting rules "drop default" and "keep default"
      (let ((default (column-default pgcol)))
        (setf (column-default pgcol)
              (cond
                ((and (stringp default) (string= "NULL" default))
                 :null)

                ((and (stringp default)
                      ;; address CURRENT_TIMESTAMP(6) and other spellings
                      (or (uiop:string-prefix-p "CURRENT_TIMESTAMP" default)
                          (string= "CURRENT TIMESTAMP" default)
                          (string= "current_timestamp()" default)))
                 :current-timestamp)

                (t (column-default pgcol)))))

      ;; extra user-defined data types
      (when (or (string-equal "set" dtype)
                (string-equal "enum" dtype))
        ;;
        ;; SET and ENUM both need more care, if the target is PostgreSQL we
        ;; need to create per-schema user defined data types that match the
        ;; column local definition here.
        ;;
        (let ((sqltype-name (enum-or-set-name table-name
                                              (column-name pgcol)
                                              dtype
                                              ctype
                                              nil)))
          ;;
          ;; We might have user-defined cast rules e.g. converting an ENUM
          ;; to text, in which case we have nothing to do here. We set a
          ;; PostgreSQL enum only when the casting rules already generated
          ;; the target type name.
          ;;
          ;; FIXME: why call enum-or-set-name twice, once from
          ;; *mysql-default-cast-rules* and once here explicitely?
          ;;
          (when (string= sqltype-name (column-type-name pgcol))
            (setf (column-type-name pgcol)
                  (make-sqltype :name sqltype-name
                                :schema (table-schema table)
                                :type (intern (string-upcase dtype)
                                              (find-package "KEYWORD"))
                                :source-def ctype
                                :extra (explode-mysql-enum ctype))))))

      ;; extra triggers
      ;;
      ;; See src/pgsql/pgsql-trigger.lisp
      ;;
      (when (eq (column-extra pgcol) :on-update-current-timestamp)
        (setf (column-extra pgcol)
              (make-trigger :name :on-update-current-timestamp)))

      pgcol)))


;;;
;;; MySQL specific testing.
;;;
;;; TODO: move that to general testing.
;;;
(defun test-casts ()
  "Just test some cases for the casts"
  (let ((*cast-rules*
	 '( ;; (:source (:column "col1" :auto-increment nil)
	   ;;  :target (:type "timestamptz"
	   ;; 	     :drop-default nil
	   ;; 	     :drop-not-null nil)
	   ;;  :using nil)

	   (:source (:type "varchar" :auto-increment nil)
	    :target (:type "text" :drop-default nil :drop-not-null nil)
	    :using nil)

	   (:source (:type "char" :typemod (= precision 1))
	    :target (:type "char" :drop-typemod nil))

           (:source (:column ("table" . "g"))
            :target nil
            :using pgloader.transforms::empty-string-to-null)))

	(columns
	 ;; name dtype       ctype         default nullable extra
	 '(("a"  "int"       "int(7)"      nil "NO" "auto_increment")
	   ("b"  "int"       "int(10)"     nil "NO" "auto_increment")
	   ("c"  "varchar"   "varchar(25)" nil  nil nil)
	   ("d"  "tinyint"   "tinyint(4)"  "0" nil nil)
	   ("e"  "datetime"  "datetime"    "0000-00-00 00:00:00" nil nil)
	   ("f"  "date"      "date"        "0000-00-00" "NO" nil)
	   ("g"  "enum"      "ENUM('a', 'b')"  nil nil nil)
	   ("h"  "set"       "SET('a', 'b')"   nil nil nil)
	   ("i"  "int"       "int(11)"         nil nil nil)
	   ("j"  "float"     "float(12,2)"     nil nil nil)
	   ("k"  "double"    "double unsigned" nil nil nil)
	   ("l"  "bigint"    "bigint(20)"      nil nil nil)
	   ("m"  "numeric"   "numeric(18,3)"   nil nil nil)
	   ("n"  "decimal"   "decimal(15,5)"   nil nil nil)
	   ("o"  "timestamp" "timestamp" "CURRENT_TIMESTAMP" "NO" "on update CURRENT_TIMESTAMP")
	   ("p"  "point"     "point"     nil "YES" nil)
	   ("q"  "char"      "char(5)"   nil "YES" nil)
	   ("l"  "char"      "char(1)"   nil "YES" nil)
	   ("m"  "integer"   "integer(4)"    nil "YES" nil)
	   ("o"  "tinyint"   "tinyint(1)" "0" nil nil)
	   ("p"  "smallint"  "smallint(5) unsigned" nil "no" "auto_increment")
	   ("q"  "mediumint" "integer"   nil "NO" "auto_increment")
	   ("r"  "tinyint"   "integer"   nil "NO" "auto_increment"))))

    ;;
    ;; format-pgsql-column when given a mysql-column would call `cast' for
    ;; us, but here we want more control over the ouput, so we call it
    ;; ourselves.
    ;;
    (format t "   ~a~30T~a~65T~a~%" "MySQL ctype" "PostgreSQL type" "transform")
    (format t "   ~a~30T~a~65T~a~%" "-----------" "---------------" "---------")
    (loop
       :for (name dtype ctype nullable default extra) :in columns
       :for mycol := (make-mysql-column "table" name nil dtype ctype nullable default extra)
       :for pgcol := (cast mycol)
       :do (format t "~a: ~a~30T~a~65T~:[~;using ~a~]~%" name ctype
                   (format-column pgcol)
                   (pgloader.pgsql::column-transform pgcol)
                   (pgloader.pgsql::column-transform pgcol)))))

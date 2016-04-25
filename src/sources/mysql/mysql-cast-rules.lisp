;;;
;;; Tools to handle MySQL data type casting rules
;;;

(in-package :pgloader.mysql)

;;;
;;; Some functions to deal with ENUM and SET types
;;;
(defun explode-mysql-enum (ctype)
  "Convert MySQL ENUM expression into a list of labels."
  (cl-ppcre:register-groups-bind (list)
      ("(?i)(?:ENUM|SET)\\s*\\((.*)\\)" ctype)
    (first (cl-csv:read-csv list :separator #\, :quote #\' :escape "\\"))))

(defun get-enum-type-name (table-name column-name)
  "Return the Type Name we're going to use in PostgreSQL."
  (apply-identifier-case (format nil "~a_~a" table-name column-name)))

(defun get-create-enum (table-name column-name ctype)
  "Return a PostgreSQL CREATE ENUM TYPE statement from MySQL enum column."
  (with-output-to-string (s)
    (format s "CREATE TYPE ~a AS ENUM (~{'~a'~^, ~});"
	    (get-enum-type-name table-name column-name)
	    (explode-mysql-enum ctype))))

(defun cast-enum (table-name column-name type ctype typemod)
  "Cast MySQL inline ENUM type to using a PostgreSQL named type.

   The type naming is hardcoded to be table-name_column-name"
  (declare (ignore type ctype typemod))
  (get-enum-type-name table-name column-name))

(defun cast-set (table-name column-name type ctype typemod)
  "Cast MySQL inline SET type to using a PostgreSQL ENUM Array.

   The ENUM data type name is hardcoded to be table-name_column-name"
  (declare (ignore type ctype typemod))
  (format nil "\"~a_~a\"[]" table-name column-name))

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

    (:source (:type "bit" :typemod (= 1 precision))
	     :target (:type "boolean" :drop-typemod t)
	     :using pgloader.transforms::bits-to-boolean)

    ;; bigint(20) unsigned (or not, actually) does not fit into PostgreSQL
    ;; bigint (-9223372036854775808 to +9223372036854775807):
    (:source (:type "bigint" :typemod (< 19 precision))
     :target (:type "numeric" :drop-typemod t))

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

    (:source (:type "datetime" :default "0000-00-00 00:00:00" :not-null t)
     :target (:type "timestamptz" :drop-default t :drop-not-null t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "datetime" :default "0000-00-00 00:00:00")
     :target (:type "timestamptz" :drop-default t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "timestamp" :default "0000-00-00 00:00:00" :not-null t)
     :target (:type "timestamptz" :drop-default t :drop-not-null t)
     :using pgloader.transforms::zero-dates-to-null)

    (:source (:type "timestamp" :default "0000-00-00 00:00:00")
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
     :target (:type ,#'cast-enum))

    (:source (:type "set")
     :target (:type ,#'cast-set)
     :using pgloader.transforms::set-to-enum-array)

    ;; geometric data types, just POINT for now
    (:source (:type "point")
     :target (:type "point")
     :using pgloader.transforms::convert-mysql-point))
  "Data Type Casting rules to migrate from MySQL to PostgreSQL")


;;;
;;; MySQL specific fields representation and low-level
;;;
(defstruct (mysql-column
	     (:constructor make-mysql-column
			   (table-name name comment dtype ctype default nullable extra)))
  table-name name dtype ctype default nullable extra comment)

(defmethod format-extra-type ((col mysql-column) &key include-drop)
  "Return a string representing the extra needed PostgreSQL CREATE TYPE
   statement, if such is needed"
  (let ((dtype (mysql-column-dtype col)))
    (when (or (string-equal "enum" dtype)
	      (string-equal "set" dtype))
      (list
       (when include-drop
	 (let* ((type-name
		 (get-enum-type-name (mysql-column-table-name col)
				     (mysql-column-name col))))
           (format nil "DROP TYPE IF EXISTS ~a CASCADE;" type-name)))

       (get-create-enum (mysql-column-table-name col)
			(mysql-column-name col)
			(mysql-column-ctype col))))))

(defmethod format-extra-triggers ((table table) (col mysql-column) &key drop)
  "Return a list of string representing the extra SQL commands needed to
   implement some MySQL features as PostgreSQL triggers, such as on update
   CURRENT_TIMESTAMP.

   When drop is t, only output the DROP sql statements."
  (when (string= (mysql-column-extra col) "on update CURRENT_TIMESTAMP")
    (let* ((field-pos (position (mysql-column-name col)
                                (table-field-list table)
                                :key #'mysql-column-name
                                :test #'string=))
           (col-name (funcall #'column-name
                              (nth field-pos (table-column-list table))))
           (fun-name (format nil "on_update_current_timestamp_~a" col-name))
           (update-fun-sql
            (format nil "
CREATE OR REPLACE FUNCTION ~a()
 RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
   NEW.~a = now();
   RETURN NEW;
END;
$$;"
                    fun-name col-name))
           (trigger-sql
            (format nil "
CREATE TRIGGER on_update_current_timestamp
 BEFORE UPDATE ON ~a
  FOR EACH ROW EXECUTE PROCEDURE ~a();"
                    (format-table-name table) fun-name)))
      (if drop
          (list
           ;; don't DROP the function here, because it might be shared by
           ;; several tables with "on update" rules on a field sharing the
           ;; same name (such as "last_update").
           ;; the CREATE OR REPLACE FUNCTION will then be innoffensive.
           (format nil "DROP TRIGGER IF EXISTS on_update_current_timestamp ON ~a;"
                   (mysql-column-table-name col)))
          (list update-fun-sql trigger-sql)))))

(defmethod cast ((col mysql-column))
  "Return the PostgreSQL type definition from given MySQL column definition."
  (with-slots (table-name name dtype ctype default nullable extra comment)
      col
    (let ((pgcol
           (apply-casting-rules table-name name dtype ctype default nullable extra)))
      (setf (column-comment pgcol) comment)
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

;;;
;;; Tools to handle IBM PC version of IXF file format
;;;
;;; http://www-01.ibm.com/support/knowledgecenter/SSEPGG_10.5.0/com.ibm.db2.luw.admin.dm.doc/doc/r0004667.html

(in-package :pgloader.ixf)

(defclass ixf-connection (fd-connection) ()
  (:documentation "pgloader connection parameters for IXF files."))

(defmethod initialize-instance :after ((ixfconn ixf-connection) &key)
  "Assign the type slot to dbf."
  (setf (slot-value ixfconn 'type) "ixf"))

(defmethod open-connection ((ixfconn ixf-connection) &key)
  (setf (conn-handle ixfconn)
        (open (fd-path ixfconn)
              :direction :input
              :element-type '(unsigned-byte 8)))
  ixfconn)

(defmethod close-connection ((ixfconn ixf-connection))
  (close (conn-handle ixfconn))
  (setf (conn-handle ixfconn) nil)
  ixfconn)

(defvar *ixf-pgsql-type-mapping*
  '((#. ixf:+smallint+  . "smallint")
    (#. ixf:+integer+   . "integer")
    (#. ixf:+bigint+    . "bigint")

    (#. ixf:+decimal+   . "numeric")
    (#. ixf:+float+     . "double precision")

    (#. ixf:+timestamp+ . "timestamptz")
    (#. ixf:+date+      . "date")
    (#. ixf:+time+      . "time")

    (#. ixf:+char+      . "text")
    (#. ixf:+varchar+   . "text")

    (#. ixf:+dbclob-location-spec+ . "text")))

(defun cast-ixf-type (ixf-type)
  "Return the PostgreSQL type name for a given IXF type name."
  (let ((pgtype
         (cdr (assoc ixf-type *ixf-pgsql-type-mapping*))))
    (unless pgtype
      (error "IXF Type mapping unknown for: ~d" ixf-type))
    pgtype))

(defun format-default-value (default)
  "IXF has some default values that we want to transform here, statically."
  (cond ((string= "CURRENT TIMESTAMP" default) "CURRENT_TIMESTAMP")
        (t default)))

(defmethod format-pgsql-column ((col ixf:ixf-column))
  "Return a string reprensenting the PostgreSQL column definition"
  (let* ((column-name (apply-identifier-case (ixf:ixf-column-name col)))
         (type-definition
          (format nil
                  "~a~:[ not null~;~]~:[~*~; default ~a~]"
                  (cast-ixf-type (ixf:ixf-column-type col))
                  (ixf:ixf-column-nullable col)
                  (ixf:ixf-column-has-default col)
                  (format-default-value (ixf:ixf-column-default col)))))

    (format nil "~a ~22t ~a" column-name type-definition)))

(defun list-all-columns (ixf-stream table-name)
  "Return the list of columns for the given IXF-FILE-NAME."
  (ixf:with-ixf-stream (ixf ixf-stream)
    (list (cons table-name
                (coerce (ixf:ixf-table-columns (ixf:ixf-file-table ixf))
                        'list)))))

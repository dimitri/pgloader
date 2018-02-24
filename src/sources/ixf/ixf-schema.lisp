;;;
;;; Tools to handle IBM PC version of IXF file format
;;;
;;; http://www-01.ibm.com/support/knowledgecenter/SSEPGG_10.5.0/com.ibm.db2.luw.admin.dm.doc/doc/r0004667.html

(in-package :pgloader.source.ixf)

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

(defmethod clone-connection ((c ixf-connection))
  (change-class (call-next-method c) 'ixf-connection))

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

    (#. ixf:+blob-location-spec+   . "bytea")
    (#. ixf:+dbblob-location-spec+ . "bytea")
    (#. ixf:+dbclob-location-spec+ . "text")))

(defun cast-ixf-type (ixf-type)
  "Return the PostgreSQL type name for a given IXF type name."
  (let ((pgtype
         (cdr (assoc ixf-type *ixf-pgsql-type-mapping*))))
    (unless pgtype
      (error "IXF Type mapping unknown for: ~d" ixf-type))
    pgtype))

(defun transform-function (field)
  "Return the transformation functions needed to cast from ixf-column data."
  (let ((coltype (cast-ixf-type (ixf:ixf-column-type field))))
    ;;
    ;; The IXF driver we use maps the data type and gets
    ;; back proper CL typed objects, where we only want to
    ;; deal with text.
    ;;
    (cond ((or (string-equal "float" coltype)
               (string-equal "real" coltype)
               (string-equal "double precision" coltype)
               (and (<= 7 (length coltype))
                    (string-equal "numeric" coltype :end2 7)))
           #'pgloader.transforms::float-to-string)

          ((string-equal "text" coltype)
           nil)

          ((string-equal "bytea" coltype)
           #'pgloader.transforms::byte-vector-to-bytea)

          (t
           (lambda (c)
             (when c
               (princ-to-string c)))))))

(defmethod cast ((col ixf:ixf-column) &key &allow-other-keys)
  "Return the PostgreSQL type definition from given IXF column definition."
  (make-column :name (apply-identifier-case (ixf:ixf-column-name col))
               :type-name (cast-ixf-type (ixf:ixf-column-type col))
               :nullable (ixf:ixf-column-nullable col)
               :default (when (ixf:ixf-column-has-default col)
                          (format-default-value
                           (ixf:ixf-column-default col)))
               :transform (transform-function col)
               :comment (let ((comment (ixf:ixf-column-desc col)))
                          (unless (or (null comment)
                                      (string= comment ""))
                            comment))))

(defun list-all-columns (ixf-stream table)
  "Return the list of columns for the given IXF-FILE-NAME."
  (ixf:with-ixf-stream (ixf ixf-stream)
    (loop :for field :across (ixf:ixf-table-columns (ixf:ixf-file-table ixf))
       :do (add-field table field))))

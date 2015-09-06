;;;
;;; Generic API for pgloader sources
;;;
(in-package :pgloader.schema)

(defmacro push-to-end (item place)
  `(setf ,place (nconc ,place (list ,item))))

;;;
;;; TODO: stop using anonymous data structures for database catalogs,
;;; currently list of alists of lists... the madness has found its way in
;;; lots of places tho.
;;;

;;;
;;; A database catalog is a list of schema each containing a list of tables,
;;; each being a list of columns.
;;;
;;; Column structures details depend on the specific source type and are
;;; implemented in each source separately.
;;;
(defstruct schema name tables)
(defstruct table schema name qname columns)

;;;
;;; Still lacking round tuits here, so for the moment the representation of
;;; a table name is either a string or a cons built from schema and
;;; table-name.
;;;

(defmacro with-schema ((var table-name) &body body)
  "When table-name is a CONS, SET search_path TO its CAR and return its CDR,
   otherwise just return the TABLE-NAME. A PostgreSQL connection must be
   established when calling this function."
  `(let ((,var
          (typecase ,table-name
            (cons   (let ((sql (format nil "SET search_path TO ~a;"
                                       (car ,table-name))))
                      (log-message :notice "~a" sql)
                      (pgloader.pgsql:pgsql-execute sql)
                      (cdr ,table-name)))
            (string ,table-name))))
     ,@body))



;;;
;;; PostgreSQL column and type output
;;;

(in-package #:pgloader.utils)

(defun format-default-value (default &optional transform)
  "Returns suitably quoted default value for CREATE TABLE command."
  (cond
    ((null default) "NULL")
    ((and (stringp default) (string= "NULL" default)) default)
    ((and (stringp default) (string= "CURRENT_TIMESTAMP" default)) default)
    ((and (stringp default) (string= "CURRENT TIMESTAMP" default))
     "CURRENT_TIMESTAMP")
    ((and (stringp default) (string= "newsequentialid()" default))
     "uuid_generate_v1()")
    (t
     ;; apply the transformation function to the default value
     (if transform (format-default-value
                    (handler-case
                        (funcall transform default)
                      (condition (c)
                        (log-message :warning
                                     "Failed to transform default value ~s: ~a"
                                     default c)
                        ;; can't transform: return nil
                        nil)))
	 (format nil "'~a'" default)))))

(defmethod format-column ((column column))
  "Format the PostgreSQL data type."
  (format nil
          "~a ~22t ~a~:[~*~;~a~]~:[ not null~;~]~:[~; default ~a~]"
          (column-name column)
          (column-type-name column)
          (column-type-mod column)
          (column-type-mod column)
          (column-nullable column)
          (column-default column)
          (column-default column)))

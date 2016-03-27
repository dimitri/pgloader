;;;
;;; PostgreSQL column and type output
;;;

(in-package #:pgloader.utils)

(defun format-default-value (default &optional transform)
  "Returns suitably quoted default value for CREATE TABLE command."
  (cond
    ((null default) "NULL")
    ((and (stringp default) (string= "NULL" default)) default)
    ((and (stringp default)
          ;; address CURRENT_TIMESTAMP(6) and other spellings
          (or (uiop:string-prefix-p "CURRENT_TIMESTAMP" default)
              (string= "CURRENT TIMESTAMP" default)))
     "CURRENT_TIMESTAMP")
    ((and (stringp default) (or (string= "newid()" default)
                                (string= "newsequentialid()" default)))
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

(defgeneric format-column (column &key pretty-print)
  (:documentation "Format COLUMN definition for CREATE TABLE purpose."))

(defmethod format-column ((column column)
                          &key
                            pretty-print
                            ((:max-column-name-length max)))
  "Format the PostgreSQL data type."
  (format nil
          "~a~vt~a~:[~*~;~a~]~:[ not null~;~]~:[~; default ~a~]"
          (column-name column)
          (if pretty-print (or (+ 2 max) 22) 1)
          (column-type-name column)
          (column-type-mod column)
          (column-type-mod column)
          (column-nullable column)
          (column-default column)
          (column-default column)))

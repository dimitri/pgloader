;;;
;;; Project fields into columns
;;;
(in-package #:pgloader.sources)

(defun project-fields (&key fields columns (compile t))
  "The simplest projection happens when both FIELDS and COLS are nil: in
   this case the projection is an identity, we simply return what we got.

   Other forms of projections consist of forming columns with the result of
   applying a transformation function. In that case a cols entry is a list
   of '(colname type expression), the expression being the (already
   compiled) function to use here."
  (labels ((null-as-processing-fn (null-as)
	     "return a lambda form that will process a value given NULL-AS."
	     (if (eq null-as :blanks)
		 (lambda (col)
		   (declare (optimize speed))
		   (if (every (lambda (char) (char= char #\Space)) col)
		       nil
		       col))
		 (lambda (col)
		   (if (string= null-as col) nil col))))

	   (field-name-as-symbol (field-name-or-list)
	     "we need to deal with symbols as we generate code"
	     (typecase field-name-or-list
	       (list (intern-symbol (car field-name-or-list)))
	       (t    (intern-symbol field-name-or-list))))

	   (process-field (field-name-or-list)
	     "Given a field entry, return a function dealing with nulls for it"
	     (destructuring-bind (&key null-as
                                       date-format
                                       trim-both
                                       trim-left
                                       trim-right
                                       &allow-other-keys)
		 (typecase field-name-or-list
		   (list (cdr field-name-or-list))
		   (t    (cdr (assoc field-name-or-list fields
                                     :test #'string-equal))))
               ;; now prepare a function of a column
               (lambda (col)
                 (let ((value-or-null
                        (if (null null-as) col
                            (funcall (null-as-processing-fn null-as) col))))
                   (when value-or-null
                     (let ((value-or-null
                            (cond (trim-both
                                   (string-trim '(#\Space) value-or-null))
                                  (trim-left
                                   (string-left-trim '(#\Space) value-or-null))
                                  (trim-right
                                   (string-right-trim '(#\Space) value-or-null))
                                  (t          value-or-null))))
                       ;; now apply the date format, when given
                       (if date-format
                           (parse-date-string value-or-null
                                              (parse-date-format date-format))
                           value-or-null))))))))

    (let* ((projection
	    (cond
	      ;; when no specific information has been given on FIELDS and
	      ;; COLUMNS, just apply generic NULL-AS processing
	      ((and (null fields) (null columns))
	       (lambda (row) (coerce row 'vector)))

	      ((null columns)
	       ;; when no specific information has been given on COLUMNS,
	       ;; use the information given for FIELDS and apply per-field
	       ;; null-as, or the generic one if none has been given for
	       ;; that field.
	       (let ((process-nulls
		      (mapcar (function process-field) fields)))
		 `(lambda (row)
                    (let ((v (make-array (length row))))
                      (loop
                         :for i :from 0
                         :for col :in row
                         :for fn :in ',process-nulls
                         :do (setf (aref v i) (funcall fn col)))
                      v))))

	      (t
	       ;; project some number of FIELDS into a possibly different
	       ;; number of COLUMNS, using given transformation functions,
	       ;; processing NULL-AS represented values.
	       (let* ((args
		       (if fields
			   (mapcar (function field-name-as-symbol) fields)
			   (mapcar (function field-name-as-symbol) columns)))
                      (values
                       ;; make sure we apply fields level processing before
                       ;; we pass in the processed field values to the
                       ;; transformation functions, if any (null if blanks)
                       (loop for field-name in args
                          collect (list
                                   field-name
                                   `(funcall ,(process-field field-name)
                                             ,field-name))))
		      (newrow
		       (loop for (name type sexp) in columns
			  collect
			  ;; we expect the name of a COLUMN to be the same
			  ;; as the name of its derived FIELD when we
			  ;; don't have any transformation function
                            (typecase sexp
                              (null   (field-name-as-symbol name))
                              (string (if (assoc sexp fields :test #'string=)
                                          ;; col text using "Field-Name"
                                          (field-name-as-symbol sexp)
                                          ;; col text using "Constant String"
                                          sexp))
                              (t      sexp)))))
		 `(lambda (row)
		    (declare (type list row))
		    (destructuring-bind (&optional ,@args &rest extra) row
		      (declare (ignorable ,@args) (ignore extra))
                      (let ,values
                        (declare (ignorable ,@args))
                        (vector ,@newrow)))))))))
      ;; allow for some debugging
      (if compile (compile nil projection) projection))))

(defun reformat-then-process (&key fields columns target)
  "Return a lambda form to apply to each row we read.

   The lambda closes over the READ paramater, which is a counter of how many
   lines we did read in the file."
  (let ((projection (project-fields :fields fields :columns columns)))
    (lambda (row)
      ;; cl-csv returns (nil) for an empty line
      (if (or (null row)
              (and (null (car row)) (null (cdr row))))
          (log-message :notice "Skipping empty line.")

          (handler-case
              (funcall projection row)
            (condition (e)
              (update-stats :data target :errs 1)
              (log-message :error "Could not read input: ~a" e)))))))


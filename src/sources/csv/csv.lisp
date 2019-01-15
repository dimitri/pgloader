;;;
;;; Tools to handle MySQL data fetching
;;;

(in-package :pgloader.source.csv)

(defclass csv-connection (md-connection) ())

(defmethod initialize-instance :after ((csv csv-connection) &key)
  "Assign the type slot to sqlite."
  (setf (slot-value csv 'type) "csv"))

;;;
;;; Implementing the pgloader source API
;;;
(defclass copy-csv (md-copy)
  ((source-type :accessor source-type	  ; one of :inline, :stdin, :regex
		:initarg :source-type)	  ;  or :filename
   (separator   :accessor csv-separator	  ; CSV separator
	        :initarg :separator	  ;
	        :initform nil)            ;
   (newline     :accessor csv-newline     ; CSV line ending
                :initarg :newline         ;
                :initform #\Newline)
   (quote       :accessor csv-quote	  ; CSV quoting
	        :initarg :quote		  ;
	        :initform cl-csv:*quote*) ;
   (escape      :accessor csv-escape	  ; CSV quote escaping
	        :initarg :escape	  ;
	        :initform cl-csv:*quote-escape*)
   (escape-mode :accessor csv-escape-mode ; CSV quote escaping mode
	        :initarg :escape-mode     ;
	        :initform cl-csv::*escape-mode*)
   (trim-blanks :accessor csv-trim-blanks ; CSV blank and NULLs
		:initarg :trim-blanks	  ;
		:initform t))
  (:documentation "pgloader CSV Data Source"))

(defmethod clone-copy-for ((csv copy-csv) path-spec)
  "Create a copy of CSV for loading data from PATH-SPEC."
  (let ((csv-for-path-spec
         (change-class (call-next-method csv path-spec) 'copy-csv)))
    (loop :for slot-name :in '(source-type
                               separator
                               newline
                               quote
                               escape
                               escape-mode
                               trim-blanks)
       :do (when (slot-boundp csv slot-name)
             (setf (slot-value csv-for-path-spec slot-name)
                   (slot-value csv slot-name))))

    ;; return the new instance!
    csv-for-path-spec))

;;;
;;; Read a file format in CSV format, and call given function on each line.
;;;
(defmethod parse-header ((csv copy-csv))
  "Parse the header line given csv setup."
  ;; a field entry is a list of field name and options
  (with-connection (cnx (source csv)
                        :direction :input
                        :external-format (encoding csv)
                        :if-does-not-exist nil)
    (let ((input (md-strm cnx)))
      (loop :repeat (skip-lines csv) :do (read-line input nil nil))
      (let* ((header-line (read-line input nil nil))
             (field-name-list
              (mapcar #'list            ; we need each field to be a list
                      (car              ; parsing a single line
                       (cl-csv:read-csv header-line
                                        :separator (csv-separator csv)
                                        :quote (csv-quote csv)
                                        :escape (csv-escape csv)
                                        :unquoted-empty-string-is-nil t
                                        :quoted-empty-string-is-nil nil
                                        :trim-outer-whitespace (csv-trim-blanks csv)
                                        :newline (csv-newline csv))))))
        (log-message :notice "Parsed header columns ~s" (fields csv))
        (setf (fields csv) field-name-list )))))

(defmethod process-rows ((csv copy-csv) stream process-fn)
  "Process rows from STREAM according to COPY specifications and PROCESS-FN."
  (let ((separator (csv-separator csv))
        (quote     (csv-quote csv))
        (escape    (csv-escape csv)))
    (unless separator
      ;; try to guess the CSV format
      (let ((nb-columns (length (columns csv))))
        (destructuring-bind (&key
                             ((:separator sep) #\Tab)
                             ((:quote q)       cl-csv:*quote*)
                             ((:escape esc)    cl-csv:*quote-escape*))
            (guess-csv-params stream nb-columns)
          (setf separator sep
                quote     q
                escape    esc)

          (log-message :notice "Guessed following CSV parameters:~%  ~a"
                       (format nil "~
  fields terminated by '~a',
  fields optionally enclosed by '~a',
  fields escaped by '~a'"
                               sep q esc)))))

    (handler-case
        (handler-bind ((cl-csv:csv-parse-error
                        #'(lambda (c)
                            (log-message :error "~a" c)
                            (update-stats :data (target csv) :errs 1)
                            (cl-csv::continue))))
          (cl-csv:read-csv stream
                           :row-fn process-fn
                           :separator separator
                           :quote quote
                           :escape escape
                           :escape-mode (csv-escape-mode csv)
                           :unquoted-empty-string-is-nil t
                           :quoted-empty-string-is-nil nil
                           :trim-outer-whitespace (csv-trim-blanks csv)
                           :newline (csv-newline csv)))
      (condition (e)
        (progn
          (log-message :fatal "~a" e)
          (update-stats :data (target csv) :errs 1))))))


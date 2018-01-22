;;;
;;; Tools to handle PostgreSQL data format
;;;
(in-package :pgloader.pgsql)

;;;
;;; Format row to PostgreSQL COPY format, the TEXT variant.
;;;
;;; That function or something equivalent is provided by default in
;;; cl-postgres, but we want to avoid having to compute its result more than
;;; once in case of a rejected batch. Also, we're using vectors as input to
;;; minimize data copying in certain cases, and we want to avoid a coerce
;;; call here.
;;;

(defun format-vector-row (nbcols row pre-formatted pg-escape-safe-array)
  "Add a ROW in the STREAM, formating ROW in PostgreSQL COPY TEXT format.

See http://www.postgresql.org/docs/9.2/static/sql-copy.html#AEN66609 for
details about the format, and format specs."
  (declare (type simple-array row))

  ;; first prepare an array of transformed and properly encoded columns
  (row-to-utf8-bytes row)

  ;; now escape bytes for the COPY text format
  (if pre-formatted
      ;; pre-formatted means no escaping needs to be done
      (format-pre-formatted-vector-row nbcols row)

      ;; in the general case we need to take into account PostgreSQL
      ;; escaping rules for the COPY format
      (format-and-escape-vector-row nbcols row pg-escape-safe-array)))


(defun row-to-utf8-bytes (row)
  "Apply transformation functions to ROW fields, and convert text to UTF-8
   bytes arrays. Return an array of array-of-bytes ready to be sent to
   PostgreSQL."
  (loop :for col :across row
     :for i fixnum :from 0
     :do (setf (aref row i)
               (if (or (null col) (eq :NULL col))
                   nil
                   (cl-postgres-trivial-utf-8:string-to-utf-8-bytes col)))))

(defun apply-transforms (nbcols row transform-fns)
  (loop :for i fixnum :below nbcols
     :for col :across row
     :for fun :in transform-fns
     :do (setf (aref row i)
               (if fun (funcall fun col) col))))

(defvar *pg-escape-safe-types*
  (list "integer"
        "serial"
        "bigserial"
        "smallint"
        "bigint"
        "numeric"
        "decimal"
        "real"
        "float"
        "timestamp"
        "timestamptz"
        "bit"
        "boolean"
        "cidr"
        "inet"
        "date"
        "interval"
        "uuid")
  "List of PostgreSQL data types with non-backslash, ascii-only input
   values: values that we don't need to escape.")

(defun pg-escape-safe-array (copy)
  "Given a PostgreSQL type name, return non-nil when this type is known to
   represent a plain ASCII value: a value that we don't need to convert into
   UTF-8, such as numbers and dates."
  (let* ((nbcols  (length (pgloader.sources::columns copy)))
         (result  (make-array nbcols :element-type '(integer 0 2)))
         (pgtypes (loop :for pgcol :in (table-column-list
                                        (pgloader.sources::target copy))
                     ;; column might miss in pg table definition, but we
                     ;; want to let PostgreSQL handle this kind of error.
                     :collect (when pgcol (column-type-name pgcol)))))
    (loop :for pgtype :in pgtypes
       :for i fixnum :from 0
       :do (setf (aref result i)
                 (cond
                   ;; bytea representation begins with a backslash, which
                   ;; needs special processing.
                   ((string= "bytea" pgtype)                               2)
                   ((member pgtype *pg-escape-safe-types* :test #'string=) 1)
                   (t                                                      0))))
    (log-message :debug "pg escape safe types for ~a: ~a ~a"
                 (format-table-name (pgloader.sources::target copy))
                 pgtypes
                 result)
    result))

(defun format-and-escape-vector-row (nbcols pgrow pg-escape-safe-array)
  "Prepare the PGROW to be send as a PostgreSQL COPY buffer."
  (flet ((escaped-length (escape-safe string)
           (if (null string)
               ;; NULL is \N (2 bytes) in COPY format
               2
               (case escape-safe
                 (2 (+ 1 (length string))) ; bytea begins with a \
                 (1 (length string))
                 (0 (loop :for byte :across string
                       :sum (case byte
                              (#. (char-code #\\)         2)
                              (#. (char-code #\Newline)   2)
                              (#. (char-code #\Return)    2)
                              (#. (char-code #\Tab)       2)
                              (#. (char-code #\Backspace) 2)
                              (#. (char-code #\Page)      2)
                              (t                          1))))))))
    (let* ((bytes  (+ nbcols
                      (loop :for col :across pgrow
                         :for escape-safe :across pg-escape-safe-array
                         :sum (escaped-length escape-safe col))))
           (result (make-array bytes :element-type '(unsigned-byte 8)))
           (pos    0))
      (macrolet ((byte-out (byte)
                   `(progn (setf (aref result pos) ,byte)
                           (incf pos)))
                 (esc-byte-out (byte)
                   `(progn (setf (aref result pos) #. (char-code #\\))
                           (setf (aref result (1+ pos)) ,byte)
                           (incf pos 2))))
        (loop :for col :across pgrow
           :for escape-safe :across pg-escape-safe-array
           :do (if (null col)
                   (esc-byte-out #. (char-code #\N))
                   (case escape-safe
                     (0 (loop :for byte :across col :do
                           (case byte
                             (#. (char-code #\\)         (esc-byte-out byte))
                             (#. (char-code #\Newline)   (esc-byte-out byte))
                             (#. (char-code #\Return)    (esc-byte-out byte))
                             (#. (char-code #\Tab)       (esc-byte-out byte))
                             (#. (char-code #\Backspace) (esc-byte-out byte))
                             (#. (char-code #\Page)      (esc-byte-out byte))
                             (t                          (byte-out byte)))))

                     (1 (replace result
                                 (the (simple-array (unsigned-byte 8) (*)) col)
                                 :start1 pos)
                        (incf pos (length col)))

                     (2 ;; Here we know we deal with bytea, and that starts
                        ;; with a backslach char that we need to escape...
                        ;; it's the almost-escape-safe case, because every
                        ;; other character in the stream is going to be an
                        ;; hexadecimal char.
                        (esc-byte-out (aref col 0))
                        (replace result
                                 (the (simple-array (unsigned-byte 8) (*)) col)
                                 :start1 pos
                                 :start2 1)
                        (incf pos (+ -1 (length col))))))

           ;; either column separator, Tab, or end-of-record with Newline
           (if (= bytes (+ 1 pos))
               (byte-out #. (char-code #\Newline))
               (byte-out #. (char-code #\Tab)))))

      ;; return the result and how many bytes it represents
      (values result bytes))))

(defun format-pre-formatted-vector-row (nbcols pgrow)
  "Prepare the PGROW for being sent as a PostgreSQL COPY buffer, knowing
   that the data in the PGROW is pre-formatted -- no extra escaping is
   needed."
  (flet ((col-length (col)
           ;; NULL is \N (2 bytes) in COPY format
           (if col (length col) 2)))
    (let* ((bytes  (+ nbcols (reduce #'+ pgrow :key #'col-length)))
           (result (make-array bytes :element-type '(unsigned-byte 8))))
      (loop :for start := 0 :then (+ start len 1)
         :for col :across pgrow
         :for len := (if col (length col) 2)
         :do (progn
               (if col
                   (replace result
                            (the (simple-array (unsigned-byte 8) (*)) col)
                            :start1 start)
                   ;; insert \N for a null value
                   (setf (aref result start)       #. (char-code #\\)
                         (aref result (+ 1 start)) #. (char-code #\N)))

               ;; either column separator, Tab, or Newline (end of record)
               (setf (aref result (+ start len))
                     (if (= bytes (+ start len 1))
                         #. (char-code #\Newline)
                         #. (char-code #\Tab)))))

      ;; return the result and how many bytes it represents
      (values result bytes))))

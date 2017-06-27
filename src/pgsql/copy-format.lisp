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

(defun format-vector-row (row
                          &optional
                            (transforms (make-list (length row)))
                            pre-formatted)
  "Add a ROW in the STREAM, formating ROW in PostgreSQL COPY TEXT format.

See http://www.postgresql.org/docs/9.2/static/sql-copy.html#AEN66609 for
details about the format, and format specs."
  (declare (type simple-array row))

  ;; first prepare an array of transformed and properly encoded columns
  (let* ((nbcols (length row))
         (pgrow  (make-array nbcols :element-type 'array)))
    (loop :for raw-col :across row
       :for i :from 0
       :for fn :in transforms
       :for col := (if pre-formatted raw-col
                       (if fn (funcall fn raw-col) raw-col))
       :do (setf (aref pgrow i)
                 (if (or (null col) (eq :NULL col))
                     nil
                     (cl-postgres-trivial-utf-8:string-to-utf-8-bytes col))))

    ;; now that we have all the columns, make a simple array out of them
    (if pre-formatted
        ;; pre-formatted data means we can return it as it already is
        (flet ((col-length (col)
                 ;; NULL is \N (2 bytes) in COPY format
                 (if col (length col) 2)))
          (let* ((bytes  (+ nbcols (reduce '+ (map 'list #'col-length pgrow))))
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
            (values result bytes)))

        ;; in the general case we need to take into account PostgreSQL
        ;; escaping rules for the COPY format
        (flet ((escaped-length (string)
                 (if (null string)
                     ;; NULL is \N (2 bytes) in COPY format
                     2
                     (loop :for byte :across string
                        :sum (case byte
                               (#. (char-code #\\)         2)
                               (#. (char-code #\Newline)   2)
                               (#. (char-code #\Return)    2)
                               (#. (char-code #\Tab)       2)
                               (#. (char-code #\Backspace) 2)
                               (#. (char-code #\Page)      2)
                               (t                          1))))))
          (let* ((bytes  (+ nbcols
                            (reduce '+ (map 'list #'escaped-length pgrow))))
                 (result (make-array bytes :element-type '(unsigned-byte 8)))
                 (pos    0))
            (macrolet ((byte-out (byte)
                         `(progn (setf (aref result pos) ,byte)
                                 (incf pos)))
                       (esc-byte-out (byte)
                         `(progn (setf (aref result pos) #. (char-code #\\))
                                 (setf (aref result (1+ pos)) ,byte)
                                 (incf pos 2))))
              (loop :for col :across pgrow :do
                 (if (null col)
                     (esc-byte-out #. (char-code #\N))
                     (loop :for byte :across col :do
                        (case byte
                          (#. (char-code #\\)         (esc-byte-out byte))
                          (#. (char-code #\Newline)   (esc-byte-out byte))
                          (#. (char-code #\Return)    (esc-byte-out byte))
                          (#. (char-code #\Tab)       (esc-byte-out byte))
                          (#. (char-code #\Backspace) (esc-byte-out byte))
                          (#. (char-code #\Page)      (esc-byte-out byte))
                          (t                          (byte-out byte)))))

                 ;; either column separator, Tab, or end-of-record with Newline
                 (if (= bytes (+ 1 pos))
                     (byte-out #. (char-code #\Newline))
                     (byte-out #. (char-code #\Tab)))))

            ;; return the result and how many bytes it represents
            (values result bytes))))))

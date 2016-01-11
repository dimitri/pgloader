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
  (let* ((bytes  0)
         (buffer (make-array 64         ; a cache line worth of data
                             :element-type '(unsigned-byte 8)
                             :adjustable t
                             :fill-pointer 0)))
    (macrolet ((byte-out (byte)
                 `(progn (vector-push-extend ,byte buffer)
                         (incf bytes)))
               (esc-byte-out (byte)
                 `(progn (vector-push-extend #. (char-code #\\) buffer)
                         (vector-push-extend ,byte buffer)
                         (incf bytes 2))))
      (loop
         :with nbcols := (length row)
         :for raw-col :across row
         :for i :from 1
         :for more? := (< i nbcols)
         :for fn :in transforms
         :for col := (if pre-formatted raw-col
                         (if fn (funcall fn raw-col) raw-col))
         :do
         (if (or (null col)
                 ;; still accept postmodern :NULL in "preprocessed" data
                 (eq :NULL col))
             (progn
               ;; NULL is expected as \N, two chars
               (byte-out #. (char-code #\\))
               (byte-out #. (char-code #\N)))

             (if pre-formatted
                 (let* ((data
                         (cl-postgres-trivial-utf-8:string-to-utf-8-bytes col))
                        (len (length data))
                        (newbuffer
                         (make-array (+ bytes len)
                                     :element-type '(unsigned-byte 8)
                                     :adjustable t
                                     :fill-pointer t)))
                   (replace newbuffer buffer :start1 0)
                   (replace newbuffer data :start1 bytes)
                   (setf buffer newbuffer)
                   (incf bytes len))

                 (loop
                    ;; From PostgreSQL docs:
                    ;;
                    ;; In particular, the following characters must be preceded
                    ;; by a backslash if they appear as part of a column value:
                    ;; backslash itself, newline, carriage return, and the
                    ;; current delimiter character.
                    :for byte
                    :across (cl-postgres-trivial-utf-8:string-to-utf-8-bytes col)
                    :do (case byte
                          (#. (char-code #\\)         (esc-byte-out byte))
                          (#. (char-code #\Space)     (byte-out byte))
                          (#. (char-code #\Newline)   (esc-byte-out byte))
                          (#. (char-code #\Return)    (esc-byte-out byte))
                          (#. (char-code #\Tab)       (esc-byte-out byte))
                          (#. (char-code #\Backspace) (esc-byte-out byte))
                          (#. (char-code #\Page)      (esc-byte-out byte))
                          (t                          (byte-out byte))))))

         :when more? :do (byte-out #. (char-code #\Tab))

         :finally (progn (byte-out #. (char-code #\Newline))
                         (return (values buffer bytes)))))))

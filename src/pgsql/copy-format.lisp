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
(defun format-vector-row (stream row
                          &optional
                            (transforms (make-list (length row)))
                            pre-formatted)
  "Add a ROW in the STREAM, formating ROW in PostgreSQL COPY TEXT format.

See http://www.postgresql.org/docs/9.2/static/sql-copy.html#AEN66609 for
details about the format, and format specs."
  (declare (type simple-array row))
  (let* ((bytes 0) *print-circle* *print-pretty*)
    (flet ((write-bytes (char-or-string)
             (declare (type (or character simple-string) char-or-string))
             ;; closes over stream and bytes, maintain the count
             (typecase char-or-string
               (character  (write-char char-or-string stream)
                           (incf bytes))
               (string     (princ char-or-string stream)
                           (incf bytes (length char-or-string))))))
      (declare (inline write-bytes))
      (loop
         with nbcols = (length row)
         for col across row
         for i from 1
         for more? = (< i nbcols)
         for fn in transforms
         for preprocessed-col = (if pre-formatted col
                                    (if fn (funcall fn col) col))
         do
           (if (or (null preprocessed-col)
                   ;; still accept postmodern :NULL in "preprocessed" data
                   (eq :NULL preprocessed-col))
               (progn
                 ;; NULL is expected as \N, two chars
                 (write-bytes #\\) (write-bytes #\N))
               (if pre-formatted
                   (map nil
                        (lambda (byte)
                          (if (<= 32 byte 127)
                              (write-bytes (code-char byte))
                              (write-bytes (format nil "\\~o" byte))))
                        (cl-postgres-trivial-utf-8:string-to-utf-8-bytes col))
                   (loop
                      ;; From PostgreSQL docs:
                      ;;
                      ;; In particular, the following characters must be preceded
                      ;; by a backslash if they appear as part of a column value:
                      ;; backslash itself, newline, carriage return, and the
                      ;; current delimiter character.
                      for byte across (cl-postgres-trivial-utf-8:string-to-utf-8-bytes preprocessed-col)
                      do (case (code-char byte)
                           (#\\         (progn (write-bytes #\\)
                                               (write-bytes #\\)))
                           (#\Space     (write-bytes #\Space))
                           (#\Newline   (progn (write-bytes #\\)
                                               (write-bytes #\n)))
                           (#\Return    (progn (write-bytes #\\)
                                               (write-bytes #\r)))
                           (#\Tab       (progn (write-bytes #\\)
                                               (write-bytes #\t)))
                           (#\Backspace (progn (write-bytes #\\)
                                               (write-bytes #\b)))
                           (#\Page      (progn (write-bytes #\\)
                                               (write-bytes #\f)))
                           (t
                            (if (<= 32 byte 127)
                                (write-bytes (code-char byte))
                                (write-bytes (format nil "\\~o" byte))))))))
         when more? do (write-bytes #\Tab)
         finally       (progn (write-bytes #\Newline)
                              (return bytes))))))

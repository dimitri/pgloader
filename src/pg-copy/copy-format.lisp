;;;
;;; Tools to handle PostgreSQL data format
;;;
(in-package :pgloader.pgcopy)

;;;
;;; Format row to PostgreSQL COPY format, the TEXT variant.
;;;
;;; That function or something equivalent is provided by default in
;;; cl-postgres, but we want to avoid having to compute its result more than
;;; once in case of a rejected batch. Also, we're using vectors as input to
;;; minimize data copying in certain cases, and we want to avoid a coerce
;;; call here.
;;;

(defun prepare-and-format-row (copy nbcols row)
  "Prepare given ROW in PostgreSQL COPY format"
  (let* ((row (prepare-row copy nbcols row)))
    (multiple-value-bind (pg-vector-row bytes)
        (if row
            (ecase (copy-format copy)
              (:raw     (format-vector-row nbcols row))
              (:escaped (format-escaped-vector-row nbcols row)))
            (values nil 0))

      ;; we might have to debug
      (when pg-vector-row
        (log-message :data "> ~s" (map 'string #'code-char pg-vector-row))

        (values pg-vector-row bytes)))))

(defun prepare-row (copy nbcols row)
  "Prepare given ROW by applying the pre-processing and transformation
   functions registered in the COPY context."
  (let* ((preprocessed-row (if (preprocessor copy)
                               (funcall (preprocessor copy) row)
                               row)))
    (cond ((eq :escaped (copy-format copy)) preprocessed-row)
          ((null (transforms copy))         preprocessed-row)
          (t
           (apply-transforms copy nbcols preprocessed-row (transforms copy))))))

(defun format-vector-row (nb-cols row)
  (declare (optimize (speed 3) (space 0) (debug 1) (compilation-speed 0)))
  (let* ((lens (map 'vector
                    (lambda (col)
                      (if (col-null-p col) 2 (copy-utf-8-byte-length col)))
                    row))
         (len  (+ nb-cols (reduce #'+ lens)))
         (buf  (make-array (the fixnum len) :element-type '(unsigned-byte 8))))
    (loop :for col :across row
       :for i fixnum :from 1
       :for position fixnum := 0 :then (+ position col-len 1)
       :for col-len fixnum :across lens
       :do (if (col-null-p col)
               (insert-copy-null buf position)
               (string-to-copy-utf-8-bytes col buf position))
       :do (insert-copy-separator buf (+ position col-len) i nb-cols))
    ;; return our pg vector of escaped utf8 bytes
    (values buf len)))

(defun format-escaped-vector-row (nb-cols row)
  "We've read data in the COPY format, so already escaped."
  (declare (optimize (speed 3) (space 0) (debug 1) (compilation-speed 0)))
  (let* ((lens (map 'vector
                    (lambda (col)
                      (if (col-null-p col) 2 (utf-8-byte-length col)))
                    row))
         (len  (+ nb-cols (reduce #'+ lens)))
         (buf  (make-array (the fixnum len) :element-type '(unsigned-byte 8))))
    (loop :for col :across row
       :for i :from 1
       :for position := 0 :then (+ position col-len 1)
       :for col-len :across lens
       :do (if (col-null-p col)
               (insert-copy-null buf position)

               (let ((utf-8-bytes (string-to-utf-8-bytes col)))
                 (replace buf utf-8-bytes :start1 position)))

       :do (insert-copy-separator buf (+ position col-len) i nb-cols))
    ;; return our pg vector of escaped utf8 bytes
    (values buf len)))

(declaim (inline insert-copy-separator insert-copy-null col-null-p))

(defun col-null-p (col)
  (or (null col) (eq :NULL col)))

(defun insert-copy-null (buffer position)
  "NULL is \\N in COPY format (that's 2 bytes)"
  (setf (aref buffer position)       #. (char-code #\\))
  (setf (aref buffer (+ 1 position)) #. (char-code #\N)))

(defun insert-copy-separator (buffer position col nb-cols)
  (if (< col nb-cols)
      (setf (aref buffer position) #. (char-code #\Tab))
      (setf (aref buffer position) #. (char-code #\Newline))))

(defun apply-transforms (copy nbcols row transform-fns)
  (handler-case
      (loop :for i fixnum :below nbcols
         :for col :across row
         :for fun :in transform-fns
         :do (setf (aref row i)
                   (if fun (funcall fun col) col))
         :finally (return row))
    (condition (e)
      (log-message :error "Error while formatting a row from ~s:"
                   (format-table-name (target copy)))
      (log-message :error "~a" e)
      (update-stats :data (target copy) :errs 1)
      nil)))

;;;
;;; Low Level UTF-8 handling + PostgreSQL COPY format escaping
;;;
;;; Main bits stolen from Postmodern:cl-postgres/trivial-utf-8.lisp
;;;
;;; We add PostgreSQL COPY escaping right at the same time as we do the
;;; UTF-8 preparation dance.
;;;
(defun copy-utf-8-byte-length (string)
  "Calculate the amount of bytes needed to encode a string."
  (declare (type string string)
           (optimize (speed 3) (space 0) (debug 1) (compilation-speed 0)))
  (let ((length (length string))
        (string (coerce string 'simple-string)))
    (loop :for char :across string
       :do (let ((code (char-code char)))
             (case char
               ((#\\ #\Newline #\Return #\Tab #\Backspace #\Page)
                (incf length 1))
               (otherwise
                (when (> code 127)
                  (incf length
                        (cond ((< code 2048) 1)
                              ((< code 65536) 2)
                              (t 3))))))))
    length))

(defmacro as-copy-utf-8-bytes (char writer)
  "Given a character, calls the writer function for every byte in the
encoded form of that character."
  (let ((char-code (gensym)))
    `(let ((,char-code (char-code ,char)))
       (declare (type fixnum ,char-code))
       (cond ((= ,char-code #. (char-code #\\))
              (progn (,writer #. (char-code #\\))
                     (,writer ,char-code)))
             ((= ,char-code #. (char-code #\Newline))
              (progn (,writer #. (char-code #\\))
                     (,writer ,char-code)))
             ((= ,char-code #. (char-code #\Return))
              (progn (,writer #. (char-code #\\))
                     (,writer ,char-code)))
             ((= ,char-code #. (char-code #\Tab))
              (progn (,writer #. (char-code #\\))
                     (,writer ,char-code)))
             ((= ,char-code #. (char-code #\Backspace))
              (progn (,writer #. (char-code #\\))
                     (,writer ,char-code)))
             ((= ,char-code #. (char-code #\Page))
              (progn (,writer #. (char-code #\\))
                     (,writer ,char-code)))
             ((< ,char-code 128)
              (,writer ,char-code))
             ((< ,char-code 2048)
              (,writer (logior #b11000000 (ldb (byte 5 6) ,char-code)))
              (,writer (logior #b10000000 (ldb (byte 6 0) ,char-code))))
             ((< ,char-code 65536)
              (,writer (logior #b11100000 (ldb (byte 4 12) ,char-code)))
              (,writer (logior #b10000000 (ldb (byte 6 6) ,char-code)))
              (,writer (logior #b10000000 (ldb (byte 6 0) ,char-code))))
             (t
              (,writer (logior #b11110000 (ldb (byte 3 18) ,char-code)))
              (,writer (logior #b10000000 (ldb (byte 6 12) ,char-code)))
              (,writer (logior #b10000000 (ldb (byte 6 6) ,char-code)))
              (,writer (logior #b10000000 (ldb (byte 6 0) ,char-code))))))))

(defun string-to-copy-utf-8-bytes (string buffer &optional (position 0))
  "Convert a string into an array of unsigned bytes containing its
utf-8 representation."
  (declare (type string string)
           (optimize (speed 3) (space 0) (debug 1) (compilation-speed 0)))
  (let ((string (coerce string 'simple-string)))
    (declare (type (array (unsigned-byte 8)) buffer)
             (type fixnum position))
    (macrolet ((add-byte (byte)
                 `(progn (setf (aref buffer position) ,byte)
                         (incf position))))
      (loop :for char :across string
         :do (as-copy-utf-8-bytes char add-byte)))))

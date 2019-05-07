;;;
;;; Given a list of columns in PostgreSQL, try to guess the fixed format
;;; specification from a sample of the file.
;;;

(in-package :pgloader.source.fixed)

(defgeneric get-first-lines (filename-or-stream &optional n)
  (:documentation "Get the first line of given FILENAME-OR-STREAM.")
  (:method ((stream stream) &optional (n 1))
    (let ((pos  (file-position stream)))
      (file-position stream 0)
      (prog1
          (loop :repeat n
             :for line := (read-line stream nil nil)
             :while line
             :collect line)
        (file-position stream pos))))
  (:method ((filename string) &optional (n 1))
    (with-open-file (stream filename
                            :direction :input
                            :external-format :utf-8
                            :if-does-not-exist nil)
      (loop :repeat n
         :for line := (read-line stream nil nil)
         :while line
         :collect line))))

(defun guess-fixed-specs-from-header (header)
  "Try to guess fixed specs from whitespace in the first line of the file."
  (let* ((size       (length header))
         current-field-name (current-field-start 0)
         specs)
    (loop :for pos :from 0
       :for previous-char := #\Space :then current-char
       :for current-char :across header
       :do (cond ((or (= (+ 1 pos) size) ; last char
                      (and (< 0 pos)     ; new field
                           (char= #\Space previous-char)
                           (char/= #\Space current-char)))
                  (when (= (+ 1 pos) size)
                    (push current-char current-field-name))
                  (push (list (map 'string #'identity
                                   (reverse current-field-name))
                              :start current-field-start
                              :length (- pos current-field-start)
                              :null-as :blanks
                              :trim-right t)
                        specs)
                  (setf current-field-name (list current-char))
                  (setf current-field-start pos))

                 ((char/= #\Space current-char)
                  (push current-char current-field-name))))
    (reverse specs)))

(defun guess-fixed-specs (filename-or-stream &optional (sample-size 1000))
  "Use the first line as an header to guess the specification of the fixed
   file from, and then match that against a sample of data from the file to
   see if that matches what data we have there."
  (let* ((sample (get-first-lines filename-or-stream sample-size))
         (header (first sample))
         (data   (rest sample))
         (fields (guess-fixed-specs-from-header header))
         (specs  (mapcar #'cdr fields)))
    (loop :for line :in data
       :collect (handler-case
                    (parse-row specs line)
                  (condition (e)
                    (log-message :error
                                 "Fixed: failed to use header as ~
specification for columns: ~a" e)
                    (return-from guess-fixed-specs nil))))
    fields))

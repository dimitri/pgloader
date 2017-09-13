;;;
;;; Random utilities
;;;
(in-package :pgloader.utils)

;;;
;;; Unquote SQLite default values, might be useful elsewhere
;;;
(defun unquote (string &optional (quote #\') (escape #\\))
  "Given '0', returns 0."
  (declare (type (or null simple-string) string))
  (when string
    (let ((l (length string)))
      (cond ((and (<= 2 l)               ; "string"
                  (char= quote (aref string 0) (aref string (1- l))))
             (subseq string 1 (1- l)))

            ((and (<= 4 l)               ; \"string\"
                  (char= escape (aref string 0) (aref string (- l 2)))
                  (char= quote (aref string 1) (aref string (- l 1))))
             (subseq string 2 (- l 2)))

            (t
             string)))))

;;;
;;; Process ~/ references at run-time (not at compile time!)
;;;
(defun expand-user-homedir-pathname (namestring)
  "Expand NAMESTRING replacing leading ~ with (user-homedir-pathname)"
  (typecase namestring
    (pathname namestring)
    (string
     (cond ((or (string= "~" namestring) (string= "~/" namestring))
            (user-homedir-pathname))

           ((and (<= 2 (length namestring))
                 (char= #\~ (aref namestring 0))
                 (char= #\/ (aref namestring 1)))
            (uiop:merge-pathnames*
             (uiop:parse-unix-namestring (subseq namestring 2))
             (user-homedir-pathname)))

           (t
            (uiop:parse-unix-namestring namestring))))))

;;;
;;; For log messages
;;;
(defun pretty-print-bytes (bytes &key (unit "B"))
  "Return a string to reprensent bytes in human readable format, with units"
  (let ((bytes (or bytes 0)))
    (loop
       :for multiple :in '("T" "G" "M" "k")
       :for power :in '(40 30 20 10 1)
       :for limit := (expt 2 power)
       :until (<= limit bytes)
       :finally (return
                  (format nil "~5,1f ~a~a" (/ bytes limit) multiple unit)))))

;;;
;;; Defining ranges and partitions.
;;;
(defun split-range (min max &optional (count *rows-per-range*))
  "Split the range from MIN to MAX into sub-ranges of COUNT elements."
  (loop :for i := min :then j
     :for j := (+ i count)
     :while (< i max)
     :collect (list i (min j max))))

(defun distribute (list-of-ranges count)
  "Split a list of ranges into COUNT sublists."
  (let ((result (make-array count :element-type 'list :initial-element nil)))
    (loop :for i :from 0
       :for range :in list-of-ranges
       :do (push range (aref result (mod i count))))
    (map 'list #'reverse result )))

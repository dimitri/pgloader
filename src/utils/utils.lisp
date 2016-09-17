;;;
;;; Random utilities
;;;
(in-package :pgloader.utils)

;;;
;;; Camel Case converter
;;;
(defun camelCase-to-colname (string)
  "Transform input STRING into a suitable column name.
    lahmanID        lahman_id
    playerID        player_id
    birthYear       birth_year"
  (coerce
   (loop
      for first = t then nil
      for char across string
      for previous-upper-p = nil then char-upper-p
      for char-upper-p = (eq char (char-upcase char))
      for new-word = (and (not first) char-upper-p (not previous-upper-p))
      when (and new-word (not (char= char #\_))) collect #\_
      collect (char-downcase char))
   'string))

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

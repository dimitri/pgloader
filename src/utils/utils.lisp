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
(defun unquote (string &optional (quote #\'))
  "Given '0', returns 0."
  (declare (type (or null simple-string) string))
  (when string
    (let ((l (length string)))
      (if (char= quote (aref string 0) (aref string (1- l)))
          (subseq string 1 (1- l))
          string))))

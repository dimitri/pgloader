;;; Test cases for issue https://github.com/dimitri/pgloader/issues/22
;;;
;;;

#|
CREATE TABLE `retry` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `content` text,
  PRIMARY KEY (`id`)
);
|#

(defpackage #:pgloader.test.retry
  (:use #:cl #:pgloader.params #:pgloader.mysql)
  (:export #:produce-data))

(in-package #:pgloader.test.retry)

(defvar *inject-null-bytes*
  (coerce (loop for previous = 0 then (+ previous offset)
             for offset in '(15769 54 7 270 8752)
             collect (+ previous offset)) 'vector)
  "Line numbers in the batch where to inject erroneous data.")

(defvar *string-with-null-byte* (concatenate 'string "Hello" (list #\Nul) "World!"))

(defvar *random-string* (make-string (random 42) :initial-element #\a)
  "A random string.")

(defvar *query* "INSERT INTO `~a`(`content`) VALUES ('~a')")

(defun produce-data (&key
                       (*myconn-host* *myconn-host*)
                       (*myconn-port* *myconn-port*)
                       (*myconn-user* *myconn-user*)
                       (*myconn-pass* *myconn-pass*)
                       (dbname "retry")
                       (table-name "retry")
                       (rows 150000))
  "Produce a data set that looks like the one in issue #22."
  (with-mysql-connection (dbname)
    (let ((next-error-pos 0))
      (loop for n from 1 to rows
         for str = (if (and (< next-error-pos (length *inject-null-bytes*))
                            (= n (aref *inject-null-bytes* next-error-pos)))
                       (progn
                         (incf next-error-pos)
                         *string-with-null-byte*)
                       *random-string*)
         do (pgloader.mysql::mysql-query (format nil *query* table-name str))))))

;;;
;;; Tools to handle MySQL data fetching
;;;

(in-package :pgloader.csv)

(defparameter *csv-path-root*
  (merge-pathnames "csv/" (user-homedir-pathname)))

(defun get-pathname (dbname table-name)
  "Return a pathname where to read or write the file data"
  (make-pathname
   :directory (pathname-directory
	       (merge-pathnames (format nil "~a/" dbname) *csv-path-root*))
   :name table-name
   :type "csv"))

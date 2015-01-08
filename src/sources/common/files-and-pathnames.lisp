;;;
;;; Some common tools for file based sources, such as CSV and FIXED
;;;
(in-package #:pgloader.sources)

(defmacro with-open-file-or-stream ((&whole arguments
                                           stream filename-or-stream
                                           &key &allow-other-keys)
                                             &body body)
  "Generate a with-open-file call, or just bind STREAM varialbe to the
   FILENAME-OR-STREAM stream when this variable is of type STREAM."
  `(typecase ,filename-or-stream
     (stream (let ((,stream *standard-input*))
               ,@body))

     (t      (with-open-file (,stream ,filename-or-stream ,@(cddr arguments))
               ,@body))))

(defun get-pathname (dbname table-name &key (csv-path-root *csv-path-root*))
  "Return a pathname where to read or write the file data"
  (make-pathname
   :directory (pathname-directory
	       (merge-pathnames (format nil "~a/" dbname) csv-path-root))
   :name table-name
   :type "csv"))

(defun filter-directory (regex
			 &key
			   (keep :first) ; or :all
			   (root *csv-path-root*))
  "Walk the ROOT directory and KEEP either the :first or :all the matches
   against the given regexp."
  (let* ((candidates (pgloader.archive:get-matching-filenames root regex))
	 (candidates (ecase keep
		       (:first (when candidates (list (first candidates))))
		       (:all   candidates))))
    (unless candidates
      (error "No file matching '~a' in expanded archive in '~a'" regex root))

    (loop for candidate in candidates
       do (if (probe-file candidate) candidate
	      (error "File does not exists: '~a'." candidate))
       finally (return candidates))))

(defun get-absolute-pathname (pathname-or-regex &key (root *csv-path-root*))
  "PATHNAME-OR-REGEX is expected to be either (:regexp expression)
   or (:filename pathname). In the first case, this fonction check if the
   pathname is absolute or relative and returns an absolute pathname given
   current working directory of ROOT.

   In the second case, walk the ROOT directory and return the first pathname
   that matches the regex. TODO: consider signaling a condition when we have
   more than one match."
  (destructuring-bind (type &rest part) pathname-or-regex
    (ecase type
      (:inline   (car part))		; because of &rest
      (:stdin    *standard-input*)
      (:regex    (destructuring-bind (keep regex root) part
		   (filter-directory regex
                                     :keep keep
                                     :root (or *csv-path-root* root))))
      (:filename (let* ((filename (first part))
                        (realname
                         (if (fad:pathname-absolute-p filename) filename
                             (merge-pathnames filename root))))
		   (if (probe-file realname) realname
		       (error "File does not exists: '~a'." realname)))))))

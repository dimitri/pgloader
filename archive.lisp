;;;
;;; Tools to handle archive files, like ZIP of CSV files
;;;

(in-package #:pgloader.archive)

(defparameter *default-tmpdir*
  (let* ((tmpdir (uiop:getenv "TMPDIR"))
	 (tmpdir (or (and tmpdir (probe-file tmpdir)) "/tmp")))
    (fad:pathname-as-directory (merge-pathnames "pgloader" tmpdir)))
  "Place where to fetch and expand archives on-disk.")

(defun http-fetch-file (url &key (tmpdir *default-tmpdir*))
  "Download a file from URL into TMPDIR."
  (ensure-directories-exist tmpdir)
  (let ((archive-filename (make-pathname :directory (namestring tmpdir)
					 :name (pathname-name url)
					 :type (pathname-type url))))
    (multiple-value-bind (http-stream
			  status-code
			  headers
			  uri
			  stream
			  should-close
			  status)
	(drakma:http-request url :force-binary t :want-stream t)
      ;; TODO: check the status-code
      (declare (ignore status-code uri stream status))
      (let* ((source-stream   (flexi-streams:flexi-stream-stream http-stream))
	     (content-length
	      (parse-integer (cdr (assoc :content-length headers)))))
	(with-open-file (archive-stream archive-filename
					:direction :output
					:element-type '(unsigned-byte 8)
					:if-exists :supersede
					:if-does-not-exist :create)
	  (let ((seq (make-array content-length
				 :element-type '(unsigned-byte 8)
				 :fill-pointer t)))
	    (setf (fill-pointer seq) (read-sequence seq source-stream))
	    (write-sequence seq archive-stream)))
	(when should-close (close source-stream))))
    ;; return the pathname where we just downloaded the file
    archive-filename))

(defun expand-archive (archive-file &key (tmpdir *default-tmpdir*))
  "Expand given ARCHIVE-FILE in TMPDIR/(pathname-name ARCHIVE-FILE). Return
   the pathname where we did expand the archive file."
  (let* ((archive-name (pathname-name archive-file))
	 (archive-type
	  (intern (string-upcase (pathname-type archive-file)) :keyword))
	 (expand-directory
	  (fad:pathname-as-directory (merge-pathnames archive-name tmpdir))))
    (ensure-directories-exist expand-directory)
    (ecase archive-type
      (:zip (zip:unzip archive-file expand-directory)))
    ;; return the pathname where we did expand the archive
    expand-directory))

(defun get-matching-filenames (directory regex)
  "Apply given REGEXP to the DIRECTORY contents and return the list of
   matching files."
  (let ((matches nil)
	(start   (length (namestring directory))))
    (flet ((push-matches (pathname)
	     (when (cl-ppcre:scan regex (namestring pathname) :start start)
	       (push pathname matches))))
      (fad:walk-directory directory #'push-matches))
    matches))


;;;
;;; Attempts at DWIM implementation when given an archive, including auto
;;; discovering the contents and guessing the column names of the CSV files
;;; therein from the header (expected first line) then guessing the data
;;; types of each column from regexps.
;;;
;;; Quite adventurous and WIP.
;;;

(defun guess-data-type (value)
  "Try to guess the data type we want to use for given value. Be very crude,
   avoid being smart. Smart means you might be unable to load data because
   of a bad guess."
  (cond ((ppcre:scan "^[0-9]*[.]?[0-9]+$" value) "numeric")
	((ppcre:scan "^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$" value) "date")
	((ppcre:scan "^[0-9]{4}-[0-9]{1,2}-[0-9]{2}$" value)   "date")
	((ppcre:scan
	  "^[0-9]{4}-[0-9]{1,2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"
	  value)                                      "timestamptz")
	(t "text")))

(defun import-csv-from-zip (zip-filename
			    &key
			      (create-tables nil)
			      (truncate t)
			      (null-as ""))
  "Parse a ZIP file found at FILENAME and import all the CSV files found.

   We only try to import data from files named `*.zip`, and we consider that
   the first line of such files are containing the names of the columns to
   import."
  (declare (ignore truncate))
  (zip:with-zipfile (zip zip-filename)
    (zip:do-zipfile-entries (filename entry zip)
      (format t "file: ~a~%" filename)
      (when (string= "csv" (pathname-type filename))
	(flex:with-input-from-sequence
	    (stream (zip:zipfile-entry-contents entry))
	  (let* ((fmt       (flex:make-external-format :utf-8 :eol-style :lf))
		 (u-stream  (flex:make-flexi-stream stream :external-format fmt))
		 (header    (read-line u-stream))
		 ;; reconsider the format when the last char of the header
		 ;; is actually #\Return
		 (header
		  (if (char= #\Return (aref header (- (length header) 1)))
		      (progn
			(setf fmt
			      (flex:make-external-format :utf-8 :eol-style :crlf)
			      u-stream
			      (flex:make-flexi-stream stream :external-format fmt))
			(string-right-trim (list #\Return) header))
		      header))
		 (first-data-line (read-line u-stream))
		 ;;
		 ;; to guess the separator from the header, find the most
		 ;; frequent separator candidate
		 (sep-counts      (loop
				     for sep in pgloader.csv::*separators*
				     collect (cons sep (count sep header))))
		 (separator       (car
				   (first (sort sep-counts #'> :key #'cdr))))
		 ;;
		 ;; now get the column names
		 (col-names
		  (mapcar #'camelCase-to-colname
			  (sq:split-sequence separator header)))
		 ;;
		 ;; and the columns types: if only digits and . then it's a
		 ;; numeric, otherwise it's a text.
		 (data       (cl-csv:read-csv-row first-data-line
						  :separator separator))
		 (col-types  (mapcar #'guess-data-type data))
		 ;;
		 ;; build column definitions
		 (col-defs (mapcar (lambda (name type)
				     (format nil "~a ~a" name type))
				   col-names col-types)))

	    (format t "  separator: ~a~%    columns: ~a~%" separator col-names)
	    (when create-tables
	      (format t "CREATE TABLE ~a (~{~a~^, ~});~%"
		      (pathname-name filename) col-defs))
	    (format t "first line: ~s~%" data)

	    ;; now create the table schema and begin importing the data

	    ))))))


;;;
;;; Tools to handle archive files, like ZIP of CSV files
;;;

(in-package #:pgloader.archive)

(let (l)
  (zip:with-zipfile
      (z "/Users/dim/dev/CL/pgloader/test/lahman2012-csv.zip")
    (zip:do-zipfile-entries (name x z) (push name l))) l)

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
			  (split-sequence:split-sequence separator header))))

	    (format t "  separator: ~a~%    columns: ~a~%" separator col-names)
	    (when create-tables
	      (format t "CREATE TABLE ~a (~{~a~^, ~});~%"
		      (pathname-name filename) col-names))
	    (format t "first line: ~a~%" first-data-line)

	    ;; then from the first line of data, guess the column data types

	    ;; now create the table schema and begin importing the data

	    ))))))


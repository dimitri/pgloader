;;;
;;; Automatic guess the CSV format parameters
;;;
(in-package #:pgloader.csv)

(defparameter *separators* '(#\Tab #\, #\; #\| #\% #\^ #\! #\$)
  "Common CSV separators to try when guessing file parameters.")

(defparameter *escape-quotes* '("\\\"" "\"\"")
  "Common CSV quotes to try when guessing file parameters.")

(defun get-file-sample (filename &key (sample-size 10))
  "Return the first SAMPLE-SIZE lines in FILENAME (or less), or nil if the
   file does not exists."
  (with-open-file
      ;; we just ignore files that don't exist
      (input filename
	     :direction :input
	     :external-format :utf-8
	     :if-does-not-exist nil)
    (when input
      (loop
	 for line = (read-line input nil)
	 while line
	 repeat sample-size
	 collect line))))

(defun try-csv-params (lines cols &key separator quote escape)
  "Read LINES as CSV with SEPARATOR and ESCAPE params, and return T when
   each line in LINES then contains exactly COLS columns"
  (let ((rows (loop
		 for line in lines
		 append
		   (handler-case
		       (cl-csv:read-csv line
					:quote quote
					:separator separator
					:escape escape)
		     ((or cl-csv:csv-parse-error type-error) ()
		       nil)))))
    (and rows
	 (every (lambda (row) (= cols (length row))) rows))))

(defun guess-csv-params (filename cols &key (sample-size 10))
  "Try a bunch of field separators with LINES and return the first one that
   returns COLS number of columns"

  (let ((sample (get-file-sample filename :sample-size sample-size)))
    (loop
       for sep in *separators*
       for esc = (loop
		    for escape in *escape-quotes*
		    when (try-csv-params sample cols
					 :quote #\"
					 :separator sep
					 :escape escape)
		    do (return escape))
       when esc
       do (return (list :separator sep :quote #\" :escape esc)))))


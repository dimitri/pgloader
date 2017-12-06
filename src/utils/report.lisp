;;;
;;; Pretty print a report while doing bulk operations
;;;

(in-package :pgloader.state)

;;;
;;; Timing Formating
;;;
(defun format-interval (seconds &optional (stream t))
  "Output the number of seconds in a human friendly way"
  (multiple-value-bind (years months days hours mins secs millisecs)
      (date:decode-interval (date:encode-interval :second seconds))
    (declare (ignore millisecs))
    (format
     stream
     "~:[~*~;~d years ~]~:[~*~;~d months ~]~:[~*~;~d days ~]~:[~*~;~dh~]~:[~*~;~dm~]~5,3fs"
     (< 0 years)  years
     (< 0 months) months
     (< 0 days)   days
     (< 0 hours)  hours
     (< 0 mins)   mins
     (+ secs (- (multiple-value-bind (r q)
		    (truncate seconds 60)
		  (declare (ignore r))
		  q)
		secs)))))

(defun parse-summary-type (&optional (pathname *summary-pathname*))
  "Return the summary type we want: human-readable, csv, json."
  (if pathname
      (cond ((string= "csv"  (pathname-type pathname)) 'print-format-csv)
            ((string= "json" (pathname-type pathname)) 'print-format-json)
            ((string= "copy" (pathname-type pathname)) 'print-format-copy)
            (t 'print-format-text))
      (if (member *client-min-messages*
                  '(:notice :sql :info :debug :data))
          'print-format-verbose
          'print-format-text)))

(defun max-length-table-name (state legend)
  "Compute the max length of a table-name in the legend."
  (reduce #'max
          (mapcar #'length
                  (mapcar (lambda (entry)
                            (etypecase entry
                              (string entry)
                              (table  (format-table-name entry))))
                          (append (pgstate-tabnames (state-data state))
                                  (pgstate-tabnames (state-preload state))
                                  (pgstate-tabnames (state-postload state))
                                  (list legend))))
          :initial-value 0))

(defun report-full-summary (state legend total-secs)
  "Report the full story when given three different sections of reporting."
  (let* ((ftype  (parse-summary-type *summary-pathname*))
         (format (make-instance ftype))
         (max-label-length (max-length-table-name state legend)))

    (when (typep format 'print-format-human-readable)
      (setf (pf-max-label-length format) max-label-length)
      (setf (pf-legend format)           legend))

    ;; replace the grand total now
    (setf (state-secs state) total-secs)
    (setf (pgstate-secs (state-data state)) total-secs)

    ;; compute total amount of bytes sent
    (let* ((data        (state-data state))
           (table-list  (mapcar (lambda (table)
                                  (gethash table (pgstate-tables data)))
                                (pgstate-tabnames data)))
           (total-bytes (reduce #'+ table-list :key #'pgtable-bytes))
           (total-read  (reduce #'+ table-list :key #'pgtable-read))
           (total-rows  (reduce #'+ table-list :key #'pgtable-rows))
           (total-errs  (reduce #'+ table-list :key #'pgtable-errs)))
      (setf (state-bytes state) total-bytes)
      (setf (pgstate-bytes (state-postload state)) total-bytes)

      (setf (pgstate-read (state-postload state)) total-read)
      (setf (pgstate-rows (state-postload state)) total-rows)
      (setf (pgstate-errs (state-postload state)) total-errs))

    (pretty-print *report-stream* state format)))


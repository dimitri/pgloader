;;;
;;; Pretty print a report while doing bulk operations
;;;

(in-package :pgloader.state)

(defvar *header-line*
  "~&~v@{~A~:*~}  ---------  ---------  ---------  --------------  ---------  ---------")

(defvar *header* "~&")
(defvar *footer* "~&")
(defvar *end-of-line-format* "~%")
(defvar *max-length-table-name* 30)
(defvar *header-tname-format* "~&~v@a")
(defvar *header-stats-format* "  ~9@a  ~9@a  ~9@a  ~14@a  ~@[~9@a~]  ~@[~9@a~]")
(defvar *header-cols-format* (concatenate 'string *header-tname-format*
					  *header-stats-format*))
(defvar *header-cols-names* '("table name" "read" "imported" "errors" "time"))

(defvar *header-format-strings*
  '((:human-readable
     (:header              "~&"
      :footer              "~%"
      :end-of-line-format  "~%"
      :header-line "~&~v@{~A~:*~}  ---------  ---------  ---------  --------------  ---------  ---------"

      :header-tname-format "~&~v@a"
      :header-stats-format "  ~9@a  ~9@a  ~9@a  ~14@a  ~:[~9<~>~;~:*~9@a~] ~:[~9<~>~;~:*~9@a~]"
      :header-cols-format  "~&~v@a  ~9@a  ~9@a  ~9@a  ~14@a  ~9@a  ~9@a"
      :header-cols-names  ("table name" "read" "imported" "errors"
                                        "total time" "read" "write")))

    (:csv
     (:header              "~&"
      :footer              "~%"
      :end-of-line-format  "~%"
      :header-line         "~*~*"
      :header-tname-format "~&~*~s;"
      :header-stats-format "~s;~s;~s;~s~*~*"
      :header-cols-format  "~&~*~s;~s;~s;~s;~s"
      :header-cols-names  ("table name" "read" "imported" "errors" "time")))

    (:copy
     (:header              "~&"
      :footer              "~%"
      :end-of-line-format  "~%"
      :header-line         "~&~*~*"
      :header-tname-format "~&~*~a	"
      :header-stats-format "~s	~s	~s	~s~*~*"
      :header-cols-format  "~*~*~*~*~*~*" ; skip it
      :header-cols-names  ("table name" "read" "imported" "errors" "time")))

    (:json
     (:header              "~&["
      :footer              "~&]~%"
      :end-of-line-format  ",~%"
      :header-line         "~&~*~*"
      :header-tname-format "~& {\"table-name\": ~*~s,"
      :header-stats-format "\"read\":~s,\"imported\":~s,\"errors\":~s,\"time\":~s~@[,\"read\":~s~]~@[,\"write\":~s~]}"
      :header-cols-format  "~*~*~*~*~*~*" ; skip it
      :header-cols-names   ("table name" "read" "imported" "errors" "time")))))

(defun get-format-for (type key)
  "Return the format string to use for a given TYPE of output and KEY."
  (getf (cadr (assoc type *header-format-strings*)) key))

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

;;;
;;; Pretty printing reports in several formats
;;;
(defun report-header ()
  ;; (apply #'format *report-stream* *header-cols-format* *header-cols-names*)
  (format *report-stream*
          "~{~}"
          *header-cols-format*
          (list* *max-length-table-name*
                 *header-cols-names*))
  (format *report-stream* *header-line* *max-length-table-name* "-"))

(defun report-table-name (table-name)
  (format *report-stream*
          *header-tname-format*
          *max-length-table-name* table-name))

(defun report-results (read rows errors seconds rs ws &optional (eol t))
  (format *report-stream* *header-stats-format* read rows errors seconds rs ws)
  (when eol
    (format *report-stream* *end-of-line-format*)))

(defun report-footer (legend read rows errors seconds &optional rs ws)
  (format *report-stream* *header-line* *max-length-table-name* "-")
  (format *report-stream*
          "~{~}"
          *header-tname-format*
          (list* *max-length-table-name*
                 (list legend)))
  (report-results read rows errors
                  (format-interval seconds nil)
                  (when (and rs (not (= rs 0.0))) (format-interval rs nil))
                  (when (and ws (not (= rs 0.0))) (format-interval ws nil))
                  nil)
  (format *report-stream* *footer*))

;;;
;;; Pretty print a report from a pgtable and pgstats counters
;;;
(defun report-pgtable-stats (pgstate name)
  (with-slots (read rows errs secs rs ws) (pgstate-get-label pgstate name)
    (report-results read rows errs
                    (format-interval secs nil)
                    (when (and rs (not (= rs 0.0))) (format-interval rs nil))
                    (when (and ws (not (= ws 0.0))) (format-interval ws nil)))))

(defun report-pgstate-stats (pgstate legend)
  (with-slots (read rows errs secs rs ws) pgstate
    (report-footer legend read rows errs secs rs ws)))

;;;
;;; Pretty print the whole summary from a state
;;;
(defun report-summary (pgstate &key (header t) footer)
  "Report a whole summary."
  (when header (report-header))
  (loop
     :for label :in (reverse (pgstate-tabnames pgstate))
     :for pgtable := (gethash label (pgstate-tables pgstate))
     :do (with-slots (read rows errs secs rs ws) pgtable
           (format *report-stream*
                   *header-tname-format*
                   *max-length-table-name*
                   (etypecase label
                     (string label)
                     (table  (format-table-name label))))
           (report-results read rows errs
                           (cond ((> 0 secs) (format-interval secs nil))
                                 ((and rs ws (= 0 secs))
                                  (format-interval (max rs ws) nil))
                                 (t (format-interval secs nil)))
                           (when (and rs (not (= rs 0.0))) (format-interval rs nil))
                           (when (and ws (not (= ws 0.0))) (format-interval ws nil))))
     :finally (when footer
                (report-pgstate-stats pgstate footer))))

(defun parse-summary-type (&optional (pathname *summary-pathname*))
  "Return the summary type we want: human-readable, csv, json."
  (when pathname
    (cond ((string= "csv"  (pathname-type pathname)) :csv)
          ((string= "json" (pathname-type pathname)) :json)
          ((string= "copy" (pathname-type pathname)) :copy)
          (t :human-readable))))

(defun max-length-table-name (legend data pre post)
  "Compute the max length of a table-name in the legend."
  (reduce #'max
          (mapcar #'length
                  (mapcar (lambda (entry)
                            (etypecase entry
                              (string entry)
                              (table  (format-table-name entry))))
                          (append (pgstate-tabnames data)
                                  (pgstate-tabnames pre)
                                  (pgstate-tabnames post)
                                  (list legend))))))

(defun report-full-summary (legend sections total-secs)
  "Report the full story when given three different sections of reporting."

  (let* ((data  (getf sections :data))
         (pre   (getf sections :pre))
         (post  (getf sections :post))

         (stype                 (or (parse-summary-type *summary-pathname*)
                                    :human-readable))
         (*header*              (get-format-for stype :header))
         (*footer*              (get-format-for stype :footer))
         (*end-of-line-format*  (get-format-for stype :end-of-line-format))
         (*header-line*         (get-format-for stype :header-line))
         (*max-length-table-name* (max-length-table-name legend data pre post))
         (*header-tname-format* (get-format-for stype :header-tname-format))
         (*header-stats-format* (get-format-for stype :header-stats-format))
         (*header-cols-format*  (get-format-for stype :header-cols-format))
         (*header-cols-names*   (get-format-for stype :header-cols-names)))

    (when *header*
      (format *report-stream* *header*))

    (when (and pre (pgstate-tabnames pre))
      (report-summary pre :footer nil)
      (format *report-stream* *header-line* *max-length-table-name* "-"))

    (report-summary data :header (null pre) :footer nil)

    (when (and post (pgstate-tabnames post))
      (format *report-stream* *header-line* *max-length-table-name* "-")
      (report-summary post :header nil :footer nil))

    ;; replace the grand total now
    (setf (pgstate-secs data) total-secs)

    ;; and report the Grand Total
    (report-pgstate-stats data legend)))


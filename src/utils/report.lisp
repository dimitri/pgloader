;;;
;;; Pretty print a report while doing bulk operations
;;;

(in-package :pgloader.utils)

(defvar *header-line*
  "~&~v@{~A~:*~}  ---------  ---------  ---------  --------------")

(defvar *header* "~&")
(defvar *footer* "~&")
(defvar *end-of-line-format* "~%")
(defvar *max-length-table-name* 30)
(defvar *header-tname-format* "~&~v@a")
(defvar *header-stats-format* "  ~9@a  ~9@a  ~9@a  ~14@a")
(defvar *header-cols-format* (concatenate 'string *header-tname-format*
					  *header-stats-format*))
(defvar *header-cols-names* '("table name" "read" "imported" "errors" "time"))

(defvar *header-format-strings*
  '((:human-readable
     (:header              "~&"
      :footer              "~%"
      :end-of-line-format  "~%"
      :header-line "~&~v@{~A~:*~}  ---------  ---------  ---------  --------------"

      :header-tname-format "~&~v@a"
      :header-stats-format "  ~9@a  ~9@a  ~9@a  ~14@a"
      :header-cols-format  "~&~v@a  ~9@a  ~9@a  ~9@a  ~14@a"
      :header-cols-names  ("table name" "read" "imported" "errors" "time")))

    (:csv
     (:header              "~&"
      :footer              "~%"
      :end-of-line-format  "~%"
      :header-line         "~*~*"
      :header-tname-format "~&~*~s;"
      :header-stats-format "~s;~s;~s;~s"
      :header-cols-format  "~&~*~s;~s;~s;~s;~s"
      :header-cols-names  ("table name" "read" "imported" "errors" "time")))

    (:copy
     (:header              "~&"
      :footer              "~%"
      :end-of-line-format  "~%"
      :header-line         "~&~*~*"
      :header-tname-format "~&~*~a	"
      :header-stats-format "~s	~s	~s	~s"
      :header-cols-format  "~*~*~*~*~*~*" ; skip it
      :header-cols-names  ("table name" "read" "imported" "errors" "time")))

    (:json
     (:header              "~&["
      :footer              "~&]~%"
      :end-of-line-format  ",~%"
      :header-line         "~&~*~*"
      :header-tname-format "~& {\"table-name\": ~*~s,"
      :header-stats-format "\"read\":~s,\"imported\":~s,\"errors\":~s,\"time\":~s}"
      :header-cols-format  "~*~*~*~*~*~*" ; skip it
      :header-cols-names   ("table name" "read" "imported" "errors" "time")))))

(defun get-format-for (type key)
  "Return the format string to use for a given TYPE of output and KEY."
  (getf (cadr (assoc type *header-format-strings*)) key))

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

(defun report-results (read rows errors seconds &optional (eol t))
  (format *report-stream* *header-stats-format* read rows errors seconds)
  (when eol
    (format *report-stream* *end-of-line-format*)))

(defun report-footer (legend read rows errors seconds)
  (format *report-stream* *header-line* *max-length-table-name* "-")
  (format *report-stream*
          "~{~}"
          *header-tname-format*
          (list* *max-length-table-name*
                 (list legend)))
  (report-results read rows errors (format-interval seconds nil) nil)
  (format *report-stream* *footer*))

;;;
;;; Pretty print a report from a pgtable and pgstats counters
;;;
(defun report-pgtable-stats (pgstate name)
  (with-slots (read rows errs secs) (pgstate-get-table pgstate name)
    (report-results read rows errs (format-interval secs nil))))

(defun report-pgstate-stats (pgstate legend)
  (with-slots (read rows errs secs) pgstate
    (report-footer legend read rows errs secs)))

;;;
;;; Pretty print the whole summary from a state
;;;
(defun report-summary (&key ((:state pgstate) *state*) (header t) footer)
  "Report a whole summary."
  (when header (report-header))
  (loop
     for table-name in (reverse (pgstate-tabnames pgstate))
     for pgtable = (gethash table-name (pgstate-tables pgstate))
     do
       (with-slots (read rows errs secs) pgtable
	 (format *report-stream*
                 *header-tname-format*
                 *max-length-table-name*
                 (format-table-name table-name))
         (report-results read rows errs (format-interval secs nil)))
     finally (when footer
	       (report-pgstate-stats pgstate footer))))

(defmacro with-stats-collection ((table-name
                                  &key
                                  dbname
                                  summary
                                  use-result-as-read
                                  use-result-as-rows
                                  ((:state pgstate) *state*))
				 &body forms)
  "Measure time spent in running BODY into STATE, accounting the seconds to
   given DBNAME and TABLE-NAME"
  (let ((result (gensym "result"))
        (secs   (gensym "secs")))
    `(prog2
         (pgstate-add-table ,pgstate ,dbname ,table-name)
         (multiple-value-bind (,result ,secs)
             (timing ,@forms)
           (cond ((and ,use-result-as-read ,use-result-as-rows)
                  (pgstate-incf ,pgstate ,table-name
                                :read ,result :rows ,result :secs ,secs))
                 (,use-result-as-read
                  (pgstate-incf ,pgstate ,table-name :read ,result :secs ,secs))
                 (,use-result-as-rows
                  (pgstate-incf ,pgstate ,table-name :rows ,result :secs ,secs))
                 (t
                  (pgstate-incf ,pgstate ,table-name :secs ,secs)))
           ,result)
       (when ,summary (report-summary)))))

(defun parse-summary-type (&optional (pathname *summary-pathname*))
  "Return the summary type we want: human-readable, csv, json."
  (when pathname
    (cond ((string= "csv"  (pathname-type pathname)) :csv)
          ((string= "json" (pathname-type pathname)) :json)
          ((string= "copy" (pathname-type pathname)) :copy)
          (t :human-readable))))

(defun report-full-summary (legend state
			    &key before finally parallel start-time)
  "Report the full story when given three different sections of reporting."

  (let* ((stype                 (or (parse-summary-type *summary-pathname*)
                                    :human-readable))
         (*header*              (get-format-for stype :header))
         (*footer*              (get-format-for stype :footer))
         (*end-of-line-format*  (get-format-for stype :end-of-line-format))
         (*header-line*         (get-format-for stype :header-line))
         (*max-length-table-name*
          (reduce #'max
                  (mapcar #'length
                          (mapcar #'format-table-name
                                  (append (pgstate-tabnames state)
                                          (when before (pgstate-tabnames before))
                                          (when finally (pgstate-tabnames finally))
                                          (when parallel (pgstate-tabnames parallel))
                                          (list legend))))))
         (*header-tname-format* (get-format-for stype :header-tname-format))
         (*header-stats-format* (get-format-for stype :header-stats-format))
         (*header-cols-format*  (get-format-for stype :header-cols-format))
         (*header-cols-names*   (get-format-for stype :header-cols-names)))

    (when *header*
      (format *report-stream* *header*))

    ;; BEFORE
    (if before
        (progn
          (report-summary :state before :footer nil)
          (format *report-stream* *header-line* *max-length-table-name* "-")
          (report-summary :state state :header nil :footer nil))
        ;; no state before
        (report-summary :state state :footer nil))

    (when (or finally parallel)
      (format *report-stream* *header-line* *max-length-table-name* "-")
      (when parallel
        (report-summary :state parallel :header nil :footer nil))
      (when finally
        (report-summary :state finally :header nil :footer nil)))

    ;; add to the grand total the other sections, except for the parallel one
    (incf (pgloader.utils::pgstate-secs state)
          (+ (if before  (pgloader.utils::pgstate-secs before)  0)
             (if finally (pgloader.utils::pgstate-secs finally) 0)))

    ;; if the parallel tasks took longer than the rest cumulated, the total
    ;; waiting time actually was parallel - before
    (if start-time
        (setf (pgloader.utils::pgstate-secs state)
              (pgloader.utils::elapsed-time-since start-time))
        (when (and parallel
                   (< (pgloader.utils::pgstate-secs state)
                      (pgloader.utils::pgstate-secs parallel)))
          (setf (pgloader.utils::pgstate-secs state)
                (- (pgloader.utils::pgstate-secs parallel)
                   (if before (pgloader.utils::pgstate-secs before) 0)))))

    ;; and report the Grand Total
    (report-pgstate-stats state legend)))


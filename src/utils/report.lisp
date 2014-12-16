;;;
;;; Pretty print a report while doing bulk operations
;;;

(in-package :pgloader.utils)

(defvar *header-line*
  "~&------------------------------  ---------  ---------  ---------  --------------")

(defvar *header-tname-format* "~&~30@a")
(defvar *header-stats-format* "  ~9@a  ~9@a  ~9@a  ~14@a")
(defvar *header-cols-format* (concatenate 'string *header-tname-format*
					  *header-stats-format*))
(defvar *header-cols-names* '("table name" "read" "imported" "errors" "time"))

(defun report-header ()
  ;; (apply #'format *report-stream* *header-cols-format* *header-cols-names*)
  (format *report-stream* "~{~}" *header-cols-format* *header-cols-names*)
  (terpri)
  (format *report-stream* *header-line*)
  (terpri))

(defun report-table-name (table-name)
  (format *report-stream* *header-tname-format* table-name))

(defun report-results (read rows errors seconds)
  (format *report-stream* *header-stats-format*
          read rows errors (format-interval seconds nil))
  (terpri))

(defun report-footer (legend read rows errors seconds)
  (terpri)
  (format *report-stream* *header-line*)
  (format *report-stream* "~{~}" *header-cols-format*
          (list legend read rows errors (format-interval seconds nil)))
  (format *report-stream* "~&")
  (terpri))

;;;
;;; Pretty print a report from a pgtable and pgstats counters
;;;
(defun report-pgtable-stats (pgstate name)
  (with-slots (read rows errs secs) (pgstate-get-table pgstate name)
    (report-results read rows errs secs)))

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
	 (format *report-stream* *header-cols-format*
		 table-name read rows errs (format-interval secs nil)))
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
  (destructuring-bind (&key ((:dbname pgconn-dbname)) &allow-other-keys)
      *pgconn*
    (let ((result (gensym "result"))
          (secs   (gensym "secs"))
          (dbname (or dbname pgconn-dbname)))
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
         (when ,summary (report-summary))))))

(defun report-full-summary (legend state
			    &key before finally parallel)
  "Report the full story when given three different sections of reporting."

  (terpri)

  ;; BEFORE
  (if before
      (progn
	(report-summary :state before :footer nil)
	(format *report-stream* pgloader.utils::*header-line*)
	(report-summary :state state :header nil :footer nil))
      ;; no state before
      (report-summary :state state :footer nil))

  (when (or finally parallel)
    (format *report-stream* pgloader.utils::*header-line*)
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
  (when (and parallel
	     (< (pgloader.utils::pgstate-secs state)
		(pgloader.utils::pgstate-secs parallel)))
    (setf (pgloader.utils::pgstate-secs state)
	  (- (pgloader.utils::pgstate-secs parallel)
	     (if before (pgloader.utils::pgstate-secs before) 0))))

  ;; and report the Grand Total
  (report-pgstate-stats state legend))


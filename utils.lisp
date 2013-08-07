;;;
;;; Random utilities
;;;
(in-package :pgloader.utils)

;;;
;;; Logs
;;;
;;; First define the log categories
(defcategory :critical)
(defcategory :error   (or :error :critical))
(defcategory :warning (or :warning :error))
(defcategory :notice  (or :notice :warning))
(defcategory :info    (or :info :notice))
(defcategory :debug   (or :debug :info))

;; Now define the Logger
(setf (log-manager)
      (make-instance 'log-manager :message-class 'formatted-message))

;; And a messenger to store our message into
(ensure-directories-exist (directory-namestring *log-filename*))
(start-messenger 'text-file-messenger :filename *log-filename*)

;; Announce what just happened
(log-message :notice "Starting pgloader, log system is ready.")

;;;
;;; Timing Macro
;;;
(defun elapsed-time-since (start)
  "Return how many seconds ticked between START and now"
  (/ (- (get-internal-real-time) start)
     internal-time-units-per-second))

(defmacro timing (&body forms)
  "return both how much real time was spend in body and its result"
  (let ((start (gensym))
	(end (gensym))
	(result (gensym)))
    `(let* ((,start (get-internal-real-time))
	    (,result (progn ,@forms))
	    (,end (get-internal-real-time)))
       (values ,result (/ (- ,end ,start) internal-time-units-per-second)))))

;;;
;;; Timing Formating
;;;
(defun format-interval (seconds &optional (stream t))
  "Output the number of seconds in a human friendly way"
  (multiple-value-bind (years months days hours mins secs millisecs)
      (date:decode-interval (date:encode-interval :second seconds))
    (format
     stream
     "~:[~*~;~d years ~]~:[~*~;~d months ~]~:[~*~;~d days ~]~:[~*~;~dh~]~:[~*~;~dm~]~d.~ds"
     (< 0 years)  years
     (< 0 months) months
     (< 0 days)   days
     (< 0 hours)  hours
     (< 0 mins)   mins
     secs (truncate millisecs))))

;;;
;;; Data Structures to maintain information about loading state
;;;
(defstruct pgtable
  name
  (read 0   :type fixnum)		; how many rows did we read
  (rows 0   :type fixnum)		; how many rows did we write
  (errs 0   :type fixnum)		; how many errors did we see
  (secs 0.0 :type float)		; how many seconds did it take
  reject-data reject-logs)		; files where to find reject data

(defstruct pgstate
  (tables (make-hash-table :test 'equal))
  (read 0   :type fixnum)
  (rows 0   :type fixnum)
  (errs 0   :type fixnum)
  (secs 0.0 :type float))

(defun pgstate-get-table (pgstate name)
  (gethash name (pgstate-tables pgstate)))

(defun pgstate-add-table (pgstate dbname table-name)
  "Instanciate a new pgtable structure to hold our stats, and return it."
  (or (pgstate-get-table pgstate table-name)
      (let* ((table (setf (gethash table-name (pgstate-tables pgstate))
			  (make-pgtable :name table-name)))
	     (reject-dir (pathname-directory
			  (merge-pathnames
			   (format nil "~a/" dbname) *reject-path-root*)))
	     (data-pathname
	      (make-pathname :directory reject-dir :name table-name :type "dat"))
	     (logs-pathname
	      (make-pathname :directory reject-dir :name table-name :type "log")))

	;; create the per-database directory if it does not exists yet
	(ensure-directories-exist (directory-namestring data-pathname))

	;; rename the existing files if there are some
	(with-open-file (data data-pathname
			      :direction :output
			      :if-exists :rename
			      :if-does-not-exist nil))

	(with-open-file (logs logs-pathname
			      :direction :output
			      :if-exists :rename
			      :if-does-not-exist nil))

	;; set the properties to the right pathnames
	(setf (pgtable-reject-data table) data-pathname
	      (pgtable-reject-logs table) logs-pathname)
	table)))

(defun pgstate-setf (pgstate name &key read rows errs secs)
  (let ((pgtable (pgstate-get-table pgstate name)))
    (when read
      (setf (pgtable-read pgtable) read)
      (incf (pgstate-read pgstate) read))
    (when rows
      (setf (pgtable-rows pgtable) rows)
      (incf (pgstate-rows pgstate) rows))
    (when errs
      (setf (pgtable-errs pgtable) errs)
      (incf (pgstate-errs pgstate) errs))
    (when secs
      (setf (pgtable-secs pgtable) secs)
      (incf (pgstate-secs pgstate) secs))
    pgtable))

(defun pgstate-incf (pgstate name &key read rows errs secs)
  (let ((pgtable (pgstate-get-table pgstate name)))
    (when read
      (incf (pgtable-read pgtable) read)
      (incf (pgstate-read pgstate) read))
    (when rows
      (incf (pgtable-rows pgtable) rows)
      (incf (pgstate-rows pgstate) rows))
    (when errs
      (incf (pgtable-errs pgtable) errs)
      (incf (pgstate-errs pgstate) errs))
    (when secs
      (incf (pgtable-secs pgtable) secs)
      (incf (pgstate-secs pgstate) secs))
    pgtable))

(defun pgstate-decf (pgstate name &key read rows errs secs)
  (let ((pgtable (pgstate-get-table pgstate name)))
    (when read
      (decf (pgtable-read pgtable) read)
      (decf (pgstate-read pgstate) read))
    (when rows
      (decf (pgtable-rows pgtable) rows)
      (decf (pgstate-rows pgstate) rows))
    (when errs
      (decf (pgtable-errs pgtable) errs)
      (decf (pgstate-errs pgstate) errs))
    (when secs
      (decf (pgtable-secs pgtable) secs)
      (decf (pgstate-secs pgstate) secs))
    pgtable))

;;;
;;; Pretty print a report while doing bulk operations
;;;
(defvar *header-line*
  "~&------------------------------  ---------  ---------  ---------  ---------")

(defvar *header-tname-format* "~&~30@a")
(defvar *header-stats-format* "  ~9@a  ~9@a  ~9@a  ~9@a")
(defvar *header-cols-format* (concatenate 'string *header-tname-format*
					  *header-stats-format*))
(defvar *header-cols-names* '("table name" "read" "imported" "errors" "time"))

(defun report-header ()
  (apply #'format t *header-cols-format* *header-cols-names*)
  (format t *header-line*))

(defun report-table-name (table-name)
  (format t *header-tname-format* table-name))

(defun report-results (read rows errors seconds)
  (format t *header-stats-format* read rows errors (format-interval seconds nil)))

(defun report-footer (legend read rows errors seconds)
  (format t *header-line*)
  (apply #'format t *header-cols-format*
	 (list legend read rows errors (format-interval seconds nil))))

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
;;; File utils
;;;
(defun slurp-file-into-string (filename)
  "Return given filename's whole content as a string."
  (with-open-file (stream filename
			  :direction :input
			  :external-format :utf-8)
    (let ((seq (make-array (file-length stream)
			   :element-type 'character
			   :fill-pointer t)))
      ;; apparently the fastest way at that is read-sequence
      ;; http://www.ymeme.com/slurping-a-file-common-lisp-83.html
      (setf (fill-pointer seq) (read-sequence seq stream))
      seq)))

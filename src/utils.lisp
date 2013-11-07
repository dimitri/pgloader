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

;; Start a messenger to store our message into
(defun start-logger (&key (log-filename *log-filename*))
  "Start the parch log manager and messenger."
  (let ((log-pathname (typecase log-filename
			(string (pathname log-filename))
			(t      log-filename))))
    (ensure-directories-exist (directory-namestring log-filename))
    (cl-log:start-messenger 'text-file-messenger
			    :filter *log-min-messages*
			    :filename log-filename)
    (cl-log:start-messenger 'text-stream-messenger
			    :filter *client-min-messages*
			    :stream *standard-output*)
    (format t "~&Now logging in '~a'.~%" log-pathname)
    (log-message :notice "Starting pgloader, log system is ready.")))

;; monkey patch the print-object method for cl-log timestamp
(defconstant +nsec+ (* 1000 1000 1000)
  "How many nanoseconds do you find in a second")

(defun fraction-to-nsecs (fraction)
  "FRACTION is a number of internal-time-units-per-second, return nsecs"
  (declare (inline fraction-to-nsecs) (fixnum fraction))
  (floor (/ (* fraction +nsec+) internal-time-units-per-second)))

(defmethod print-object ((self cl-log:timestamp) stream)
  "we want to print human readable timestamps"
  (let ((log-local-time
	 (local-time:universal-to-timestamp
	  (cl-log:timestamp-universal-time self)
	  :nsec (fraction-to-nsecs (cl-log:timestamp-fraction self)))))
    (local-time:format-timestring stream log-local-time)))

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
  "~&------------------------------  ---------  ---------  ---------  --------------")

(defvar *header-tname-format* "~&~30@a")
(defvar *header-stats-format* "  ~9@a  ~9@a  ~9@a  ~14@a")
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
;;; Pretty print the whole summary from a state
;;;
(defun report-summary (&key ((:state pgstate) *state*) (header t) footer)
  "Report a whole summary."
  (when header (report-header))
  (loop
     for table-name being the hash-keys in (pgstate-tables pgstate)
     using (hash-value pgtable)
     do
       (with-slots (read rows errs secs) pgtable
	 (format t *header-cols-format*
		 table-name read rows errs (format-interval secs nil)))
     finally (when footer
	       (report-pgstate-stats pgstate footer))))

(defmacro with-stats-collection ((dbname table-name
					 &key
					 summary
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
	   (if ,use-result-as-rows
	       (pgstate-incf ,pgstate ,table-name :rows ,result :secs ,secs)
	       (pgstate-incf ,pgstate ,table-name :secs ,secs))
	   ,result)
       (when ,summary (report-summary)))))

(defun report-full-summary (legend state
			    &key before finally parallel)
  "Report the full story when given three different sections of reporting."

  ;; BEFORE
  (if before
      (progn
	(report-summary :state before :footer nil)
	(format t pgloader.utils::*header-line*)
	(report-summary :state state :header nil :footer nil))
      ;; no state before
      (report-summary :state state :footer nil))

  (when (or finally parallel)
    (format t pgloader.utils::*header-line*)
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

;;;
;;; Camel Case converter
;;;
(defun camelCase-to-colname (string)
  "Transform input STRING into a suitable column name.
    lahmanID        lahman_id
    playerID        player_id
    birthYear       birth_year"
  (coerce
   (loop
      for first = t then nil
      for char across string
      for previous-upper-p = nil then char-upper-p
      for char-upper-p = (eq char (char-upcase char))
      for new-word = (and (not first) char-upper-p (not previous-upper-p))
      when (and new-word (not (char= char #\_))) collect #\_
      collect (char-downcase char))
   'string))

;;;
;;; lparallel
;;;
(defun make-kernel (worker-count
		    &key (bindings
			  `((*pgconn-host* . ,*pgconn-host*)
			    (*pgconn-port* . ,*pgconn-port*)
			    (*pgconn-user* . ,*pgconn-user*)
			    (*pgconn-pass* . ,*pgconn-pass*)
			    (*pg-settings* . ',*pg-settings*)
			    (*myconn-host* . ,*myconn-host*)
			    (*myconn-port* . ,*myconn-port*)
			    (*myconn-user* . ,*myconn-user*)
			    (*myconn-pass* . ,*myconn-pass*)
			    (*state*       . ,*state*))))
  "Wrapper around lparallel:make-kernel that sets our usual bindings."
  (lp:make-kernel worker-count :bindings bindings))

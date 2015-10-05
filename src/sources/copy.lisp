;;;
;;; Read a file format in PostgreSQL COPY TEXT format.
;;;
(in-package :pgloader.copy)

(defclass copy-connection (md-connection) ())

(defmethod initialize-instance :after ((copy copy-connection) &key)
  "Assign the type slot to sqlite."
  (setf (slot-value copy 'type) "copy"))

(defclass copy-copy (md-copy)
  ((encoding    :accessor encoding	  ; file encoding
	        :initarg :encoding)	  ;
   (skip-lines  :accessor skip-lines	  ; we might want to skip COPY lines
	        :initarg :skip-lines	  ;
		:initform 0)              ;
   (delimiter   :accessor delimiter       ; see COPY options for TEXT
                :initarg :delimiter       ; in PostgreSQL docs
                :initform #\Tab)
   (null-as     :accessor null-as
                :initarg :null-as
                :initform "\\N"))
  (:documentation "pgloader COPY Data Source"))

(defmethod initialize-instance :after ((copy copy-copy) &key)
  "Compute the real source definition from the given source parameter, and
   set the transforms function list as needed too."
  (let ((transforms (when (slot-boundp copy 'transforms)
		      (slot-value copy 'transforms)))
	(columns
         (or (slot-value copy 'columns)
             (pgloader.pgsql:list-columns (slot-value copy 'target-db)
                                          (slot-value copy 'target)))))
    (unless transforms
      (setf (slot-value copy 'transforms) (make-list (length columns))))))

(declaim (inline parse-row))

(defun parse-row (line &key (delimiter #\Tab) (null-as "\\N"))
  "Parse a single line of COPY input file and return a row of columns."
  (mapcar (lambda (x)
            ;; we want Postmodern compliant NULLs
            (cond ((string= null-as x) :null)

                  ;; and we want to avoid injecting default NULL
                  ;; representation down to PostgreSQL when null-as isn't
                  ;; the default
                  ((and (string/= null-as "\\N") (string= x "\\N"))
                   ;; escape the backslash
                   "\\\\N")

                  ;; default case, just use the value we've just read
                  (t x)))
          ;; splitting is easy, it's always on #\Tab
          ;; see format-row-for-copy for details
          (sq:split-sequence delimiter line)))

(defmethod process-rows ((copy copy-copy) stream process-fn)
  "Process rows from STREAM according to COPY specifications and PROCESS-FN."
  (loop
     :with fun := process-fn
     :for line := (read-line stream nil nil)
     :counting line :into read
     :while line
     :do (handler-case
             (funcall fun (parse-row line
                                     :delimiter (delimiter copy)
                                     :null-as   (null-as copy)))
           (condition (e)
             (progn
               (log-message :error "~a" e)
               (update-stats :data (target copy) :errs 1))))))

(defmethod copy-to-queue ((copy copy-copy) queue)
  "Copy data from given COPY definition into lparallel.queue DATAQ"
  (pgloader.queue:map-push-queue copy queue 'pre-formatted))



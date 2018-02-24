;;;
;;; Read a file format in PostgreSQL COPY TEXT format.
;;;
(in-package :pgloader.source.copy)

(defclass copy-connection (md-connection) ())

(defmethod initialize-instance :after ((copy copy-connection) &key)
  "Assign the type slot to sqlite."
  (setf (slot-value copy 'type) "copy"))

(defclass copy-copy (md-copy)
  ((delimiter   :accessor delimiter       ; see COPY options for TEXT
                :initarg :delimiter       ; in PostgreSQL docs
                :initform #\Tab)
   (null-as     :accessor null-as
                :initarg :null-as
                :initform "\\N"))
  (:documentation "pgloader COPY Data Source"))

(defmethod clone-copy-for ((copy copy-copy) path-spec)
  "Create a copy of FIXED for loading data from PATH-SPEC."
  (let ((copy-for-path-spec
         (change-class (call-next-method copy path-spec) 'copy-copy)))
    (loop :for slot-name :in '(delimiter null-as)
       :do (when (slot-boundp copy slot-name)
             (setf (slot-value copy-for-path-spec slot-name)
                   (slot-value copy slot-name))))

    ;; return the new instance!
    copy-for-path-spec))

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

(defmethod data-is-preformatted-p ((copy copy-copy)) t)


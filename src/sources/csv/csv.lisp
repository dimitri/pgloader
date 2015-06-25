;;;
;;; Tools to handle MySQL data fetching
;;;

(in-package :pgloader.csv)

(defclass csv-connection (fd-connection)
  ((specs :initarg :specs :accessor csv-specs)))

(defmethod initialize-instance :after ((csvconn csv-connection) &key)
  "Assign the type slot to sqlite."
  (setf (slot-value csvconn 'type) "csv"))

(defmethod print-object ((csv csv-connection) stream)
  (print-unreadable-object (csv stream :type t :identity t)
    (let ((specs (if (slot-boundp csv 'specs) (slot-value csv 'specs)
                     `(:http ,(slot-value csv 'pgloader.connection::uri)))))
      (with-slots (type) csv
        (format stream "~a://~a:~a" type (first specs) (second specs))))))

(defmethod expand :after ((csv csv-connection))
  "Expand the archive for the FD connection."
  (when (and (slot-boundp csv 'pgloader.connection::path)
             (slot-value csv 'pgloader.connection::path)
             (uiop:file-pathname-p (fd-path csv)))
    (setf (csv-specs csv) `(:filename ,(fd-path csv)))))

(defmethod fetch-file :after ((csv csv-connection))
  "When the fd-connection has an URI slot, download its file."
  (when (and (slot-boundp csv 'pgloader.connection::path)
             (slot-value csv 'pgloader.connection::path))
    (setf (csv-specs csv) `(:filename ,(fd-path csv)))))

;;;
;;; Implementing the pgloader source API
;;;
(defclass copy-csv (copy)
  ((source-type :accessor source-type	  ; one of :inline, :stdin, :regex
		:initarg :source-type)	  ;  or :filename
   (encoding    :accessor encoding	  ; file encoding
	        :initarg :encoding)	  ;
   (csv-header  :accessor csv-header      ; CSV headers are col names
                :initarg :csv-header
                :initform nil)            ;
   (skip-lines  :accessor skip-lines	  ; CSV skip firt N lines
	        :initarg :skip-lines	  ;
		:initform 0)		  ;
   (separator   :accessor csv-separator	  ; CSV separator
	        :initarg :separator	  ;
	        :initform #\Tab)	  ;
   (newline     :accessor csv-newline     ; CSV line ending
                :initarg :newline         ;
                :initform #\Newline)
   (quote       :accessor csv-quote	  ; CSV quoting
	        :initarg :quote		  ;
	        :initform cl-csv:*quote*) ;
   (escape      :accessor csv-escape	  ; CSV quote escaping
	        :initarg :escape	  ;
	        :initform cl-csv:*quote-escape*)
   (escape-mode :accessor csv-escape-mode ; CSV quote escaping mode
	        :initarg :escape-mode     ;
	        :initform cl-csv::*escape-mode*)
   (trim-blanks :accessor csv-trim-blanks ; CSV blank and NULLs
		:initarg :trim-blanks	  ;
		:initform t))
  (:documentation "pgloader CSV Data Source"))

(defmethod initialize-instance :after ((csv copy-csv) &key)
  "Compute the real source definition from the given source parameter, and
   set the transforms function list as needed too."
  (let ((source (csv-specs (slot-value csv 'source))))
    (setf (slot-value csv 'source-type) (car source))
    (setf (slot-value csv 'source)      (get-absolute-pathname source)))

  (let ((transforms (when (slot-boundp csv 'transforms)
		      (slot-value csv 'transforms)))
	(columns
	 (or (slot-value csv 'columns)
	     (pgloader.pgsql:list-columns (slot-value csv 'target-db)
					  (slot-value csv 'target)))))
    (unless transforms
      (setf (slot-value csv 'transforms) (make-list (length columns))))))

;;;
;;; Read a file format in CSV format, and call given function on each line.
;;;
(defun parse-csv-header (csv header)
  "Parse the header line given csv setup."
  ;; a field entry is a list of field name and options
  (mapcar #'list
          (car                          ; parsing a single line
           (cl-csv:read-csv header
                            :separator (csv-separator csv)
                            :quote (csv-quote csv)
                            :escape (csv-escape csv)
                            :unquoted-empty-string-is-nil t
                            :quoted-empty-string-is-nil nil
                            :trim-outer-whitespace (csv-trim-blanks csv)
                            :newline (csv-newline csv)))))

(defmethod map-rows ((csv copy-csv) &key process-row-fn)
  "Load data from a text file in CSV format, with support for advanced
   projecting capabilities. See `project-fields' for details.

   Each row is pre-processed then PROCESS-ROW-FN is called with the row as a
   list as its only parameter.

   FILESPEC is either a filename or a pair (filename . position) where
   position is the number of bytes to skip in the file before getting to the
   data. That's used to handle the INLINE data loading.

   Finally returns how many rows where read and processed."
  (let ((filenames   (case (source-type csv)
                       (:stdin   (list (source csv)))
                       (:inline  (list (car (source csv))))
                       (:regex   (source csv))
                       (t        (list (source csv))))))
    (loop for filename in filenames
       do
	 (with-open-file-or-stream
	     ;; we just ignore files that don't exist
	     (input filename
		    :direction :input
		    :external-format (encoding csv)
		    :if-does-not-exist nil)
	   (when input
	     (log-message :info "COPY FROM ~s" filename)

	     ;; first go to given inline position when filename is :inline
	     (when (eq (source-type csv) :inline)
	       (file-position input (cdr (source csv))))

	     ;; we handle skipping more than one line here, as cl-csv only knows
	     ;; about skipping the first line
	     (loop repeat (skip-lines csv) do (read-line input nil nil))

             ;; we might now have to read the CSV fields from the header line
             (when (csv-header csv)
               (setf (fields csv)
                     (parse-csv-header csv (read-line input nil nil)))

               (log-message :debug "Parsed header columns ~s" (fields csv)))

	     ;; read in the text file, split it into columns, process NULL
	     ;; columns the way postmodern expects them, and call
	     ;; PROCESS-ROW-FN on them
	     (let ((reformat-then-process
		    (reformat-then-process :fields  (fields csv)
					   :columns (columns csv)
					   :target  (target csv)
					   :process-row-fn process-row-fn)))
               (handler-case
                   (handler-bind ((cl-csv:csv-parse-error
                                   #'(lambda (c)
                                       (log-message :error "~a" c)
                                       (cl-csv::continue))))
                     (cl-csv:read-csv input
                                      :row-fn (compile nil reformat-then-process)
                                      :separator (csv-separator csv)
                                      :quote (csv-quote csv)
                                      :escape (csv-escape csv)
                                      :escape-mode (csv-escape-mode csv)
                                      :unquoted-empty-string-is-nil t
                                      :quoted-empty-string-is-nil nil
                                      :trim-outer-whitespace (csv-trim-blanks csv)
                                      :newline (csv-newline csv)))
                 (condition (e)
                   (progn
                     (log-message :error "~a" e)
                     (pgstate-incf *state* (target csv) :errs 1))))))))))

(defmethod copy-to-queue ((csv copy-csv) queue)
  "Copy data from given CSV definition into lparallel.queue DATAQ"
  (map-push-queue csv queue))

(defmethod copy-from ((csv copy-csv) &key truncate disable-triggers)
  "Copy data from given CSV file definition into its PostgreSQL target table."
  (let* ((summary        (null *state*))
	 (*state*        (or *state* (pgloader.utils:make-pgstate)))
	 (lp:*kernel*    (make-kernel 2))
	 (channel        (lp:make-channel))
	 (queue          (lq:make-queue :fixed-capacity *concurrent-batches*)))

    (with-stats-collection ((target csv)
                            :dbname (db-name (target-db csv))
                            :state *state* :summary summary)
      (lp:task-handler-bind () ;; ((error #'lp:invoke-transfer-error))
        (log-message :notice "COPY ~a" (target csv))
        (lp:submit-task channel #'copy-to-queue csv queue)

        ;; and start another task to push that data from the queue to PostgreSQL
        (lp:submit-task channel
                        ;; this function update :rows stats
                        #'pgloader.pgsql:copy-from-queue
                        (target-db csv) (target csv) queue
                        ;; we only are interested into the column names here
                        :columns (mapcar (lambda (col)
                                           ;; always double quote column names
                                           (format nil "~s" (car col)))
                                         (columns csv))
                        :truncate truncate
                        :disable-triggers disable-triggers)

        ;; now wait until both the tasks are over
        (loop for tasks below 2 do (lp:receive-result channel)
           finally (lp:end-kernel))))))

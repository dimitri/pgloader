;;;
;;; Global state maintenance, which includes statistics about each target of
;;; the load: number of lines read, imported and number of errors found
;;; along the way.
;;;
(in-package :pgloader.state)

;;;
;;; Data Structures to maintain information about loading state
;;;
(defstruct pgtable
  name
  (read 0   :type fixnum)		; how many rows did we read
  (rows 0   :type fixnum)		; how many rows did we write
  (errs 0   :type fixnum)		; how many errors did we see
  (secs 0.0 :type float)		; how many seconds did it take
  (rs   0.0 :type float)                ;   seconds spent reading
  (ws   0.0 :type float)                ;   seconds spent writing
  (bytes 0  :type fixnum)               ; how many bytes we sent
  (start 0  :type integer)              ; internal real time when we started
  (stop  0  :type integer)              ; internal real time when we finished
  reject-data reject-logs)		; files where to find reject data

(defstruct pgstate
  (tables   (make-hash-table :test 'equal))
  (tabnames nil)                        ; we want to keep the ordering
  (read 0   :type fixnum)
  (rows 0   :type fixnum)
  (errs 0   :type fixnum)
  (secs 0.0 :type float)
  (rs   0.0 :type float)
  (ws   0.0 :type float)
  (bytes 0  :type fixnum))

(defstruct state
  (preload  nil :type (or null pgstate))
  (data     nil :type (or null pgstate))
  (postload nil :type (or null pgstate))
  (bytes      0 :type fixnum)
  (secs     0.0 :type float))

(defun create-state ()
  (make-state :preload  (make-pgstate)
              :data     (make-pgstate)
              :postload (make-pgstate)))

(defun get-state-section (state section)
  (ecase section
    (:pre   (state-preload  state))
    (:data  (state-data     state))
    (:post  (state-postload state))))

(defun relative-pathname (filename type &optional dbname)
  "Return the pathname of a file of type TYPE (dat or log) under *ROOT-DIR*"
  (let ((dir (if dbname
                 (uiop:merge-pathnames*
                  (uiop:make-pathname* :directory `(:relative ,dbname))
                  *root-dir*)
                 *root-dir*)))
    (make-pathname :defaults dir :name filename :type type)))

(defun reject-data-file (table-name dbname)
  "Return the pathname to the reject file for STATE entry."
  (relative-pathname table-name "dat" dbname))

(defun reject-log-file (table-name dbname)
  "Return the pathname to the reject file for STATE entry."
  (relative-pathname table-name "log" dbname))

(defmethod pgtable-initialize-reject-files ((table pgtable) dbname)
  "Prepare TABLE for being able to deal with rejected rows (log them)."
  (let* ((dbname        (pgloader.quoting:ensure-unquoted dbname))
         (table-name    (pgloader.quoting:ensure-unquoted
                         (table-name (pgtable-name table))))
         (data-pathname (reject-data-file table-name dbname))
         (logs-pathname (reject-log-file table-name dbname)))
    ;; we also use that facility for things that are not tables
    ;; such as "fetch" or "before load" or "Create Indexes"
    (when dbname
      ;; create the per-database directory if it does not exists yet
      (ensure-directories-exist (uiop:pathname-directory-pathname data-pathname))

      ;; rename the existing files if there are some
      (when (probe-file data-pathname)
        (with-open-file (data data-pathname
                              :direction :output
                              :if-exists :rename
                              :if-does-not-exist nil)
          (declare (ignore data))))

      (when (probe-file logs-pathname)
        (with-open-file (logs logs-pathname
                              :direction :output
                              :if-exists :rename
                              :if-does-not-exist nil)
          (declare (ignore logs))))

      ;; set the properties to the right pathnames
      (setf (pgtable-reject-data table) data-pathname
            (pgtable-reject-logs table) logs-pathname))))

(defun pgstate-get-label (pgstate name)
  (gethash name (pgstate-tables pgstate)))

(defun pgstate-new-label (pgstate label)
  "Instanciate a new pgtable structure to hold our stats, and return it."
  (or (pgstate-get-label pgstate label)
      (let* ((pgtable (setf (gethash label (pgstate-tables pgstate))
                            (make-pgtable :name label
                                          :start (get-internal-real-time)))))

        ;; maintain the ordering
        (push label (pgstate-tabnames pgstate))

        (when (typep label 'table)
          (pgtable-initialize-reject-files pgtable (table-name label)))

	pgtable)))

(defun pgstate-setf (pgstate name &key read rows errs secs rs ws bytes)
  (let ((pgtable (pgstate-get-label pgstate name)))
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
    (when rs
      (setf (pgtable-rs pgtable) rs)
      (incf (pgstate-rs pgstate) rs))
    (when ws
      (setf (pgtable-ws pgtable) ws)
      (incf (pgstate-ws pgstate) ws))
    (when bytes
      (setf (pgtable-bytes pgtable) bytes)
      (incf (pgstate-bytes pgstate) bytes))
    pgtable))

(defun pgstate-incf (pgstate name &key read rows errs secs rs ws bytes)
  (let ((pgtable (pgstate-get-label pgstate name)))
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
    (when rs
      (incf (pgtable-rs pgtable) rs)
      (incf (pgstate-rs pgstate) rs))
    (when ws
      (incf (pgtable-ws pgtable) ws)
      (incf (pgstate-ws pgstate) ws))
    (when bytes
      (incf (pgtable-bytes pgtable) bytes)
      (incf (pgstate-bytes pgstate) bytes))
    pgtable))

(defun pgstate-decf (pgstate name &key read rows errs secs rs ws bytes)
  (let ((pgtable (pgstate-get-label pgstate name)))
    (when read
      (decf (pgtable-read pgtable) read)
      (decf (pgstate-read pgstate) read))
    (when rows
      (decf (pgtable-rows pgtable) rows)
      (decf (pgstate-rows pgstate) rows))
    (when errs
      (decf (pgtable-errs pgtable) errs)
      (decf (pgstate-errs pgstate) errs))
    (when rs
      (decf (pgtable-rs pgtable) rs)
      (decf (pgstate-rs pgstate) rs))
    (when ws
      (decf (pgtable-ws pgtable) ws)
      (decf (pgstate-ws pgstate) ws))
    (when secs
      (decf (pgtable-secs pgtable) secs)
      (decf (pgstate-secs pgstate) secs))
    (when bytes
      (decf (pgtable-bytes pgtable) secs)
      (decf (pgstate-bytes pgstate) secs))
    pgtable))

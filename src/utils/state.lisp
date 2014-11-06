;;;
;;; Global state maintenance, which includes statistics about each target of
;;; the load: number of lines read, imported and number of errors found
;;; along the way.
;;;
(in-package :pgloader.utils)

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
  (tables   (make-hash-table :test 'equal))
  (tabnames nil)                        ; we want to keep the ordering
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
	     (reject-dir    (merge-pathnames (format nil "~a/" dbname) *root-dir*))
	     (data-pathname (make-pathname :defaults reject-dir
                                           :name table-name :type "dat"))
	     (logs-pathname (make-pathname :defaults reject-dir
                                           :name table-name :type "log")))

        ;; maintain the ordering
        (push table-name (pgstate-tabnames pgstate))

	;; create the per-database directory if it does not exists yet
	(ensure-directories-exist reject-dir)

	;; rename the existing files if there are some
        (when (probe-file data-pathname)
          (with-open-file (data data-pathname
                                :direction :output
                                :if-exists :rename
                                :if-does-not-exist nil)))

        (when (probe-file logs-pathname)
          (with-open-file (logs logs-pathname
                                :direction :output
                                :if-exists :rename
                                :if-does-not-exist nil)))

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

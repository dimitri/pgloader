;;;
;;; Tools to handle archive files, like ZIP of CSV files
;;;

(in-package #:pgloader.archive)

(defun http-fetch-file (url &key (tmpdir *default-tmpdir*))
  "Download a file from URL into TMPDIR."

  ;; This operation could take some time, make it so that the user knows
  ;; it's happening for him.
  (log-message :log "Fetching '~a'" url)

  (ensure-directories-exist tmpdir)
  (let ((archive-filename (make-pathname :directory (namestring tmpdir)
					 :name (pathname-name url)
					 :type (pathname-type url))))
    (multiple-value-bind (http-stream
			  status-code
			  headers
			  uri
			  stream
			  should-close
			  status)
	(drakma:http-request url :force-binary t :want-stream t)
      ;; TODO: check the status-code
      (declare (ignore status-code uri stream status))
      (let* ((source-stream   (flexi-streams:flexi-stream-stream http-stream))
	     (content-length
	      (parse-integer (cdr (assoc :content-length headers)))))
	(with-open-file (archive-stream archive-filename
					:direction :output
					:element-type '(unsigned-byte 8)
					:if-exists :supersede
					:if-does-not-exist :create)
	  (let ((seq (make-array content-length
				 :element-type '(unsigned-byte 8)
				 :fill-pointer t)))
	    (setf (fill-pointer seq) (read-sequence seq source-stream))
	    (write-sequence seq archive-stream)))
	(when should-close (close source-stream))))
    ;; return the pathname where we just downloaded the file
    archive-filename))

(defun unzip (archive-file expand-directory)
  "Unzip an archive"
  ;; TODO: fallback to the following if the unzip command is not found
  ;; (zip:unzip archive-file expand-directory :if-exists :supersede)
  (let ((command (format nil "unzip -o ~a -d ~a" archive-file expand-directory)))
    (log-message :notice "~a" command)
    (uiop:run-program command)))

(defun expand-archive (archive-file &key (tmpdir *default-tmpdir*))
  "Expand given ARCHIVE-FILE in TMPDIR/(pathname-name ARCHIVE-FILE). Return
   the pathname where we did expand the archive file."

  ;; This operation could take some time, force a message out about it.
  (log-message :log "Extracting files from archive '~a'" archive-file)

  (unless (probe-file archive-file)
    (error "File does not exists: '~a'." archive-file))

  (let* ((archive-name (pathname-name archive-file))
	 (archive-type
	  (intern (string-upcase (pathname-type archive-file)) :keyword))
	 (expand-directory
	  (fad:pathname-as-directory (merge-pathnames archive-name tmpdir))))
    (ensure-directories-exist expand-directory)
    (ecase archive-type
      (:zip (unzip archive-file expand-directory)))
    ;; return the pathname where we did expand the archive
    expand-directory))

(defun get-matching-filenames (directory regex)
  "Apply given REGEXP to the DIRECTORY contents and return the list of
   matching files."
  (let ((matches nil)
	(start   (length (namestring directory))))
    (flet ((push-matches (pathname)
	     (when (cl-ppcre:scan regex (namestring pathname) :start start)
	       (push pathname matches))))
      (fad:walk-directory directory #'push-matches))
    matches))

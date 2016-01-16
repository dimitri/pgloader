;;;
;;; Some common tools for file based sources, such as CSV and FIXED
;;;
(in-package #:pgloader.sources)

(defclass md-connection (fd-connection)
  ((spec :initarg :spec :accessor md-spec)
   (strm :initarg :strm :accessor md-strm))
  (:documentation "pgloader connection parameters for a multi-files source."))

(defmethod print-object ((c md-connection) stream)
  (print-unreadable-object (c stream :type t :identity t)
    (with-slots (type spec) c
      (let ((path (when (slot-boundp c 'path) (slot-value c 'path))))
        (etypecase path
          (string
           (format stream "~a://~a:~a" type (first spec) path))
          (list
           (format stream "~a://~a:~{~a~^,~}" type (first spec) path)))))))

(defmethod expand :after ((md md-connection))
  "Expand the archive for the MD connection."
  (when (and (slot-boundp md 'pgloader.connection::path)
             (slot-value md 'pgloader.connection::path)
             (uiop:file-pathname-p (fd-path md)))
    (setf (md-spec md) `(:filename ,(fd-path md)))))

(defmethod fetch-file :after ((md md-connection))
  "When the fd-connection has an URI slot, download its file."
  (when (and (slot-boundp md 'pgloader.connection::path)
             (slot-value md 'pgloader.connection::path))
    (setf (md-spec md) `(:filename ,(fd-path md)))))

(defgeneric expand-spec (md-connection)
  (:documentation "Expand specification for an FD source."))

(defmethod expand-spec ((md md-connection))
  "Given fd spec as a CONS of a source type and a tagged object, expand the
  tagged object depending on the source type and return a list of pathnames."
  (destructuring-bind (type &rest part) (md-spec md)
    (ecase type
      (:inline   (list (md-spec md)))
      (:stdin    (list *standard-input*))
      (:regex    (destructuring-bind (keep regex root) part
		   (filter-directory regex
                                     :keep keep
                                     :root (or *fd-path-root* root))))
      (:filename (let* ((filename (first part))
                        (realname
                         (if (fad:pathname-absolute-p filename) filename
                             (merge-pathnames filename *fd-path-root*))))
		   (if (probe-file realname) (list realname)
		       (error "File does not exists: '~a'." realname)))))))

(defmethod open-connection ((md md-connection)
                            &rest args
                            &key &allow-other-keys)
  "We know how to open several kinds of specs here, all that target a single
   file as input. The multi-file specs must have been expanded before trying
   to open the connection."
  (when (fd-path md)
    (log-message :notice "Opening ~s" (fd-path md)) ; info
    (cond ;; inline
          ((and (listp (fd-path md))
                (eq :inline (first (fd-path md))))
           (destructuring-bind (filename . position)
               (second (fd-path md))
             ;; open the filename with given extra args
             (setf (md-strm md) (apply #'open filename args))
             ;; and position the stream as expected
             (file-position (md-strm md) position)))

          ;; stdin
          ((streamp (fd-path md))
           (setf (md-strm md) (fd-path md)))

          ;; other cases should be filenames
          (t
           (setf (md-strm md) (apply #'open (fd-path md) args)))))
  md)

(defmethod close-connection ((md md-connection))
  "Reset."
  (when (and (slot-boundp md 'strm) (md-strm md))
    (close (md-strm md)))

  (setf (md-strm md) nil))

(defun get-pathname (dbname table-name &key (fd-path-root *fd-path-root*))
  "Return a pathname where to read or write the file data"
  (make-pathname
   :directory (pathname-directory
	       (merge-pathnames (format nil "~a/" dbname) fd-path-root))
   :name table-name
   :type "csv"))

(defun filter-directory (regex
			 &key
			   (keep :first) ; or :all
			   (root *fd-path-root*))
  "Walk the ROOT directory and KEEP either the :first or :all the matches
   against the given regexp."
  (let* ((candidates (pgloader.archive:get-matching-filenames root regex))
	 (candidates (ecase keep
		       (:first (when candidates (list (first candidates))))
		       (:all   candidates))))
    (unless candidates
      (error "No file matching '~a' in expanded archive in '~a'" regex root))

    (loop for candidate in candidates
       do (if (probe-file candidate) candidate
	      (error "File does not exists: '~a'." candidate))
       finally (return candidates))))


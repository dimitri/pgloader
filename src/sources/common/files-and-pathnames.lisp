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
        (format stream "~a://~a:~{~a~^,~}" type (first spec) path)))))

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

(defgeneric open-next-stream (md-connection &rest args &key)
  (:documentation "Open the next input stream from FD."))

(defmethod expand-spec ((md md-connection))
  "Given fd spec as a CONS of a source type and a tagged object, expand the
  tagged object depending on the source type and return a list of pathnames."
  (destructuring-bind (type &rest part) (md-spec md)
    (ecase type
      (:inline   (list (caar part)))
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

(defmethod open-next-stream ((md md-connection)
                             &rest args
                             &key &allow-other-keys)
  "Switch to the following file in the PATH list."
  ;; first close current stream
  (when (slot-boundp md 'strm)
    (close (md-strm md)))

  ;; now open the new one
  (when (fd-path md)
    (let ((current-path (pop (fd-path md))))
      (when current-path
        (log-message :info "Open ~s" current-path)
        (prog1
            (setf (md-strm md)
                  (typecase current-path
                    (stream current-path)
                    (t      (apply #'open current-path args))))
          ;;
          ;; The :inline md spec is a little special, it's a filename and a
          ;; position where to skip to at opening the file. It allows for
          ;; easier self-contained tests.
          ;;
          (when (eq :inline (car (md-spec md)))
            (file-position (md-strm md) (cdadr (md-spec md)))))))))

(defmethod open-connection ((md md-connection) &key)
  "The fd connection supports several specs to open a connection:
     - if we have a path, that's a single file to open
     - otherwise spec is an CONS wherin
         - car is the source type
         - cdr is an object suitable for `get-absolute-pathname`"
  (setf (fd-path md) (expand-spec md))
  md)

(defmethod close-connection ((md md-connection))
  "Reset."
  (when (and (slot-boundp md 'strm) (md-strm md))
    (close (md-strm md)))

  (setf (md-strm md) nil
        (fd-path md) nil))

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


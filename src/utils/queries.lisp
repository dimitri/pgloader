;;;
;;; Load SQL queries at load-time into an hash table and offer a function to
;;; get the SQL query text from the source code. This allows to maintain
;;; proper .sql files in the source code, for easier maintenance.
;;;

(in-package :pgloader.queries)

(defparameter *src*
  (uiop:pathname-directory-pathname
   (asdf:system-relative-pathname :pgloader "src/"))
  "Source directory where to look for .sql query files.")

(defun load-static-file (fs pathname url)
  "Load given PATHNAME contents at URL-PATH in FS."
  (setf (gethash url fs)
        (alexandria:read-file-into-string pathname)))

(defun pathname-to-url (pathname &optional (root *src*))
  "Transform given PATHNAME into an URL at which to serve it within URL-PATH."
  (multiple-value-bind (flag path-list last-component file-namestring-p)
      (uiop:split-unix-namestring-directory-components
       (uiop:unix-namestring
        (uiop:enough-pathname pathname root)))
    (declare (ignore flag file-namestring-p))
    ;;
    ;; we store SQL queries in a sql/ subdirectory because it's easier to
    ;; manage the code that way, but it's an implementation detail that we
    ;; are hiding in the query url abstraction...
    ;;
    ;; then do the same thing with the "sources" in /src/sources/.../sql/...
    ;;
    (let ((no-sql-path-list
           (remove-if (lambda (path)
                        (member path (list "sources" "sql")
                                :test #'string=))
                      path-list)))
      (format nil "~{/~a~}/~a" no-sql-path-list last-component))))

(defun load-static-directory (fs &optional (root *src*))
  "Walk PATH and load all files found in there as binary sequence, FS being
   an hash table referencing the full path against the bytes."
  (flet ((collectp  (dir) (declare (ignore dir)) t)
         (recursep  (dir) (declare (ignore dir)) t)
         (collector (dir)
           (loop :for pathname :in (uiop:directory-files dir)
              :do (when (string= "sql" (pathname-type pathname))
                    (let ((url (pathname-to-url pathname root)))
                      (load-static-file fs pathname url))))))
    (uiop:collect-sub*directories root #'collectp #'recursep #'collector)))

(defun walk-sources-and-build-fs ()
  (let ((fs (make-hash-table :test #'equal)))
    (load-static-directory fs)
    fs))

(defparameter *fs*
  (walk-sources-and-build-fs)
  "File system as an hash-table in memory.")

(defun sql (url &rest args)
  "Abstract the hash-table based implementation of our SQL file system."
  (restart-case
      (apply #'format nil
             (or (gethash url *fs*)
                 (error "URL ~s not found!" url))
             args)
    (recompute-fs-and-retry ()
      (setf *fs* (walk-sources-and-build-fs))
      (sql url))))

(defun sql-url-for-variant (base filename &optional variant)
  "Build a SQL URL for given VARIANT"
  (flet ((sql-base-url (base filename)
           (format nil "/~a/~a" base filename)))
    (if variant
        (let ((sql-variant-url
               (format nil "/~a/~a/~a"
                       base
                       (string-downcase (typecase variant
                                          (symbol (symbol-name variant))
                                          (string variant)
                                          (t      (princ-to-string variant))))
                       filename)))
          (if (gethash sql-variant-url *fs*)
              sql-variant-url
              (sql-base-url base filename)))

        (sql-base-url base filename))))

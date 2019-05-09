;;;
;;; Create a build/bin/pgloader executable from the source code, using
;;; Quicklisp to load pgloader and its dependencies.
;;;

(in-package #:cl-user)

;; ccl provides an implementation of getenv already.
#+sbcl
(defun getenv (name &optional default)
  "Return the current value for the environment variable NAME, or default
   when unset."
  (or (sb-ext:posix-getenv name) default))

;; So that we can #+pgloader-image some code away, see main.lisp
(push :pgloader-image *features*)

;;;
;;; We need to support *print-circle* for the debug traces of the catalogs,
;;; and while at it let's enforce *print-pretty* too.
;;;
(setf *print-circle* t *print-pretty* t)


(require :asdf)                         ; should work in SBCL and CCL

(defvar *quicklisp.lisp* "http://beta.quicklisp.org/quicklisp.lisp")

(let* ((cwd        (uiop:getcwd))
       (build-dir  (uiop:merge-pathnames* "build/" cwd))
       (ql.lisp    (uiop:merge-pathnames* "quicklisp.lisp" build-dir))
       (qldir      (uiop:merge-pathnames* "quicklisp/" build-dir))
       (qlsetup    (uiop:merge-pathnames* "setup.lisp" qldir)))
  ;;
  ;; We might have to install Quicklisp in build/quicklisp
  ;;
  (unless (probe-file qlsetup)
    (format t "File ~a is not found, installing Quicklisp from ~a~%"
            qlsetup *quicklisp.lisp*)
    (let ((command (format nil "curl -o ~a ~a" ql.lisp *quicklisp.lisp*)))
      (format t "Running command: ~a~%" command)
      (uiop:run-program command))
    (load ql.lisp)
    (let* ((quickstart (find-package "QUICKLISP-QUICKSTART"))
           (ql-install (find-symbol "INSTALL" quickstart)))
      (funcall ql-install :path qldir :proxy (getenv "http_proxy"))))

  ;;
  ;; Now that we have Quicklisp, load it and push our copy of pgloader in
  ;; ql:*local-project-directories* where Quicklisp will find it.
  ;;
  (format t "Loading file ~a~%" qlsetup)
  (load qlsetup)

  (let* ((ql        (find-package "QL"))
         (lpd       (find-symbol "*LOCAL-PROJECT-DIRECTORIES*" ql))
         (quickload (find-symbol "QUICKLOAD" ql)))
    (push cwd (symbol-value lpd))

    ;;
    ;; And finally load pgloader and its image-based hooks
    ;;
    (format t "Loading system pgloader~%")
    (funcall quickload :pgloader)
    (load (asdf:system-relative-pathname :pgloader "src/hooks.lisp"))))

(defun pgloader-image-main ()
  (let ((argv #+sbcl sb-ext:*posix-argv*
              #+ccl ccl:*command-line-argument-list*))
    (pgloader::main argv)))

(let* ((cwd            (uiop:getcwd))
       (build-dir      (uiop:merge-pathnames* "build/bin/" cwd))
       (image-filename (uiop:merge-pathnames* "pgloader" build-dir)))
  #+ccl
  (ccl:save-application image-filename
                        :toplevel-function #'cl-user::pgloader-image-main
                        :prepend-kernel t)
  #+sbcl
  (sb-ext:save-lisp-and-die image-filename
                            :toplevel #'cl-user::pgloader-image-main
                            :executable t
                            :save-runtime-options t
                            :compression (uiop:featurep :sb-core-compression)))

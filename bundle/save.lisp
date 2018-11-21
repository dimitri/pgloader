;;;
;;; Create a build/bin/pgloader executable from the source code, using
;;; Quicklisp to load pgloader and its dependencies.
;;;

(in-package #:cl-user)

(require :asdf)                         ; should work in SBCL and CCL

(let* ((cwd             (uiop:getcwd))
       (bundle.lisp     (uiop:merge-pathnames* "bundle.lisp" cwd))
       (version-file    (uiop:merge-pathnames* "version.sexp" cwd))
       (version-string  (uiop:read-file-form version-file))
       (asdf:*central-registry* (list cwd)))

  (format t "Loading bundle.lisp~%")
  (load bundle.lisp)

  (format t "Loading system pgloader ~a~%" version-string)
  (asdf:load-system :pgloader :verbose nil)
  (load (asdf:system-relative-pathname :pgloader "src/hooks.lisp"))

  (let* ((pgl            (find-package "PGLOADER"))
         (version-symbol (find-symbol "*VERSION-STRING*" pgl)))
    (setf (symbol-value version-symbol) version-string)))

(defun pgloader-image-main ()
  (let ((argv #+sbcl sb-ext:*posix-argv*
              #+ccl ccl:*command-line-argument-list*))
    (pgloader::main argv)))

(let* ((cwd          (uiop:getcwd))
       (bin-dir      (uiop:merge-pathnames* "bin/" cwd))
       (bin-filename (uiop:merge-pathnames* "pgloader" bin-dir)))

  (ensure-directories-exist bin-dir)

  #+ccl
  (ccl:save-application bin-filename
                        :toplevel-function #'cl-user::pgloader-image-main
                        :prepend-kernel t)
  #+sbcl
  (sb-ext:save-lisp-and-die bin-filename
                            :toplevel #'cl-user::pgloader-image-main
                            :executable t
                            :save-runtime-options t
                            :compression t))

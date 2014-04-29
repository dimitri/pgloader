;;;
;;; At SBCL image startup, we have a situation when the exact pathname for
;;; the OpenSSL shared Object is not to be found at the same location again.
;;;
;;; Hack our way around that by registering hooks to force unloading and
;;; loading the lib at proper times.
;;;
;;; Note: the culprit seems to be qmynd and its usage of :weakly-depends-on
;;; :cl+ssl in its system definition.
;;;

(in-package #:cl-user)

#+ccl
(progn
  (push (lambda () (cffi:close-foreign-library 'CL+SSL::LIBSSL))
        *save-exit-functions*)

  (push (lambda () (cffi:load-foreign-library 'CL+SSL::LIBSSL))
        *lisp-startup-functions*))

#+sbcl
(progn
 (push (lambda () (cffi:close-foreign-library 'CL+SSL::LIBSSL))
       sb-ext:*save-hooks*)

 (push (lambda () (cffi:load-foreign-library 'CL+SSL::LIBSSL))
       sb-ext:*init-hooks*))

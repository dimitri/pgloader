;;;
;;; Export a getenv feature so that we can fetch http_proxy at build time.
;;;
;;; We can't rely on Quicklisp to have installed a modern ASDF with UIOP yet
;;; here: we need the feature to pass in the :proxy argument to
;;; quicklisp-quickstart:install.
;;;

(in-package :cl-user)

(defun getenv (name &optional default)
  "Return the current value for the environment variable NAME, or default
   when unset."
  (or #+sbcl (sb-ext:posix-getenv name)
      #+ccl (ccl:getenv name)
      default))

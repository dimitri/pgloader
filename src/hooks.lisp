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

(defun close-foreign-libs ()
  "Close Foreign libs in use by pgloader at application save time."
  (let (#+sbcl (sb-ext:*muffled-warnings* 'style-warning))
    (mapc #'cffi:close-foreign-library '(cl+ssl::libssl
                                         mssql::sybdb))))

(defun open-foreign-libs ()
  "Open Foreign libs in use by pgloader at application start time."
  (let (#+sbcl (sb-ext:*muffled-warnings* 'style-warning))
    ;; we specifically don't load mssql::sybdb eagerly, it's getting loaded
    ;; in only when the data source is a MS SQL database.
    (cffi:load-foreign-library 'cl+ssl::libssl)))

#+ccl  (push #'open-foreign-libs *lisp-startup-functions*)
#+sbcl (push #'open-foreign-libs sb-ext:*save-hooks*)

#+ccl  (push #'close-foreign-libs *save-exit-functions*)
#+sbcl (push #'close-foreign-libs sb-ext:*init-hooks*)

;;;
;;; Register all loaded systems in the image, so that ASDF don't search for
;;; them again when doing --self-upgrade
;;;
(setf pgloader::*self-upgrade-immutable-systems*
      (remove "pgloader" (asdf:already-loaded-systems) :test #'string=))

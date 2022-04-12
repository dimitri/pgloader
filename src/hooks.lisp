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

;; So that we can #+pgloader-image some code away, see main.lisp
(push :pgloader-image *features*)

;;;
;;; We need to support *print-circle* for the debug traces of the catalogs,
;;; and while at it let's enforce *print-pretty* too.
;;;
(setf *print-circle* t *print-pretty* t)

(defun close-foreign-libs ()
  "Close Foreign libs in use by pgloader at application save time."
  (let (#+sbcl (sb-ext:*muffled-warnings* 'style-warning))
    (mapc #'cffi:close-foreign-library '(cl+ssl::libssl
                                         cl+ssl::libcrypto
                                         mssql::sybdb))))

(defun open-foreign-libs ()
  "Open Foreign libs in use by pgloader at application start time."
  (let (#+sbcl (sb-ext:*muffled-warnings* 'style-warning))
    ;; we specifically don't load mssql::sybdb eagerly, it's getting loaded
    ;; in only when the data source is a MS SQL database.
    ;;
    ;; and for CL+SSL, we need to call the specific reload function that
    ;; handles some context and things around loading with CFFI.
    (cl+ssl:reload)))

#+ccl  (push #'open-foreign-libs *lisp-startup-functions*)
#+sbcl (push #'open-foreign-libs sb-ext:*init-hooks*)

#+ccl  (push #'close-foreign-libs *save-exit-functions*)
#+sbcl (push #'close-foreign-libs sb-ext:*save-hooks*)

;;;
;;; Register all loaded systems in the image, so that ASDF don't search for
;;; them again when doing --self-upgrade
;;;

;;; FIXME: this idea kept failing.

#|
(defun register-preloaded-system (system)
  (unless (string= "pgloader" (asdf::coerce-name system))
    (let ((version (slot-value system 'asdf::version)))
      (asdf::register-preloaded-system system :version version))))

(asdf:map-systems #'register-preloaded-system)

(setf pgloader::*self-upgrade-immutable-systems*
      (remove "pgloader" (asdf:already-loaded-systems) :test #'string=))

(defun list-files-to-load-for-system (system-name)
  (loop for (o . c) in (asdf/plan:plan-actions
                        (asdf/plan:make-plan 'asdf/plan:sequential-plan
                                             'asdf:load-source-op
                                             (asdf:find-system system-name)))
     when (typep o 'asdf:load-source-op)
     append (asdf:input-files o c)))
|#

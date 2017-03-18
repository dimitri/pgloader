;;;
;;; For communication purposes, the lparallel kernel must be created with
;;; access to some common bindings.
;;;

(in-package :pgloader.utils)

(defun make-kernel (worker-count
		    &key (bindings
			  `((*monitoring-queue*   . ,*monitoring-queue*)
                            (*copy-batch-rows*    . ,*copy-batch-rows*)
                            (*copy-batch-size*    . ,*copy-batch-size*)
                            (*concurrent-batches* . ,*concurrent-batches*)
			    (*pg-settings*        . ',*pg-settings*)
                            (*root-dir*           . ,*root-dir*)
                            (*fd-path-root*       . ,*fd-path-root*)
                            (*client-min-messages* . ,*client-min-messages*)
                            (*log-min-messages*    . ,*log-min-messages*)

                            ;; needed in create index specific kernels
                            (*pgsql-reserved-keywords* . ',*pgsql-reserved-keywords*)
                            (*preserve-index-names* . ,*preserve-index-names*)

                            ;; bindings updates for libs
                            ;; CFFI is used by the SQLite lib
                            (cffi:*default-foreign-encoding*
                             . ,cffi:*default-foreign-encoding*))))
  "Wrapper around lparallel:make-kernel that sets our usual bindings."
  (lp:make-kernel worker-count :bindings bindings))

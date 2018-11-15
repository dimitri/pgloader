;;;
;;; For communication purposes, the lparallel kernel must be created with
;;; access to some common bindings.
;;;

(in-package :pgloader.utils)

(defun make-kernel (worker-count
		    &key (bindings
			  `((*print-circle*       . ,*print-circle*)
                            (*print-pretty*       . ,*print-pretty*)
                            (*monitoring-queue*   . ,*monitoring-queue*)
                            (*copy-batch-rows*    . ,*copy-batch-rows*)
                            (*copy-batch-size*    . ,*copy-batch-size*)
                            (*rows-per-range*     . ,*rows-per-range*)
                            (*prefetch-rows*      . ,*prefetch-rows*)
			    (*pg-settings*        . ',*pg-settings*)
			    (*mysql-settings*     . ',*mysql-settings*)
			    (*mssql-settings*     . ',*mssql-settings*)
                            (*root-dir*           . ,*root-dir*)
                            (*fd-path-root*       . ,*fd-path-root*)
                            (*client-min-messages* . ,*client-min-messages*)
                            (*log-min-messages*    . ,*log-min-messages*)

                            ;; needed in create index specific kernels
                            (*pgsql-reserved-keywords* . ',*pgsql-reserved-keywords*)
                            (*preserve-index-names* . ,*preserve-index-names*)
                            (*identifier-case*      . ,*identifier-case*)

                            ;; bindings updates for libs
                            ;; CFFI is used by the SQLite lib
                            (cffi:*default-foreign-encoding*
                             . ,cffi:*default-foreign-encoding*)

                            ;; CL+SSL can be picky about verifying certs
                            (cl+ssl:*make-ssl-client-stream-verify-default*
                             . ,cl+ssl:*make-ssl-client-stream-verify-default*))))
  "Wrapper around lparallel:make-kernel that sets our usual bindings."
  (lp:make-kernel worker-count :bindings bindings))

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
                            (*pgconn-host*        . ',*pgconn-host*)
			    (*pgconn-port*        . ,*pgconn-port*)
			    (*pgconn-user*        . ,*pgconn-user*)
			    (*pgconn-pass*        . ,*pgconn-pass*)
			    (*pg-settings*        . ',*pg-settings*)
			    (*myconn-host*        . ',*myconn-host*)
			    (*myconn-port*        . ,*myconn-port*)
			    (*myconn-user*        . ,*myconn-user*)
			    (*myconn-pass*        . ,*myconn-pass*)
			    (*msconn-host*        . ',*msconn-host*)
			    (*msconn-port*        . ,*msconn-port*)
			    (*msconn-user*        . ,*msconn-user*)
			    (*msconn-pass*        . ,*msconn-pass*)
			    (*state*              . ,*state*)
                            (*client-min-messages* . ,*client-min-messages*)
                            (*log-min-messages*    . ,*log-min-messages*)

                            ;; bindings updates for libs
                            ;; CFFI is used by the SQLite lib
                            (cffi:*default-foreign-encoding*
                             . ,cffi:*default-foreign-encoding*))))
  "Wrapper around lparallel:make-kernel that sets our usual bindings."
  (lp:make-kernel worker-count :bindings bindings))

;;;
;;; The PostgreSQL COPY TO implementation, with batches and retries.
;;;
;;; Here, sending the data in the COPY stream opened in copy-batch.
;;;
(in-package :pgloader.copy)

;;;
;;; Stream prepared data from *writer-batch* down to PostgreSQL using the
;;; COPY protocol, and retry the batch avoiding known bad rows (from parsing
;;; COPY error messages) in case some data related conditions are signaled.
;;;
(defun db-write-row (copier data)
  "Copy cl-postgres:db-write-row guts to avoid computing utf-8 bytes all
   over again, as we reproduced the data formating in pgloader code. The
   reason we do that is to be able to lower the cost of retrying batches:
   the formating has then already been done."
  (let* ((connection          (cl-postgres::copier-database copier))
	 (cl-postgres::socket (cl-postgres::connection-socket connection)))
    (cl-postgres::with-reconnect-restart connection
      (cl-postgres::using-connection connection
        (cl-postgres::with-syncing
          (cl-postgres::write-uint1 cl-postgres::socket 100)
          (cl-postgres::write-uint4 cl-postgres::socket (+ 4 (length data)))
          (loop :for byte :across data
             :do (write-byte byte cl-postgres::socket))))))
  (incf (cl-postgres::copier-count copier)))



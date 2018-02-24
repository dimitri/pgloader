;;;
;;; The PostgreSQL COPY TO implementation, with batches and retries.
;;;
;;; Here, sending the data in the COPY stream opened in copy-batch.
;;;
(in-package :pgloader.pgcopy)

(define-condition copy-init-error (error)
  ((table     :initarg :table :reader copy-init-error-table)
   (columns   :initarg :columns :reader copy-init-error-columns)
   (condition :initarg :condition :reader copy-init-error-condition))
  (:report (lambda (err stream)
             (format stream
                     "Can't init COPY to ~a~@[(~{~a~^, ~})~]: ~%~a"
                     (format-table-name (copy-init-error-table err))
                     (copy-init-error-columns err)
                     (copy-init-error-condition err)))))

;;;
;;; Stream prepared data from *writer-batch* down to PostgreSQL using the
;;; COPY protocol, and retry the batch avoiding known bad rows (from parsing
;;; COPY error messages) in case some data related conditions are signaled.
;;;
(defun db-write-batch (copier batch)
  (loop :for count :below (batch-count batch)
     :for data :across (batch-data batch)
     :do (when data
           (db-write-row copier data))
     :finally (return (batch-count batch))))

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



;;;
;;; Functions to stream a vector of rows as strings directly into the COPY
;;; socket stream, without using an intermediate buffer.
;;;
(defun db-write-vector-row (copier row &optional (nbcols (length row)))
  "Copy cl-postgres:db-write-row guts to avoid computing utf-8 bytes all
   over again, as we reproduced the data formating in pgloader code. The
   reason we do that is to be able to lower the cost of retrying batches:
   the formating has then already been done."
  (declare (optimize (speed 3) (space 0) (debug 1) (compilation-speed 0)))
  (let* ((col-bytes           (map 'vector
                                   (lambda (col)
                                     (if (col-null-p col) 2
                                         (copy-utf-8-byte-length col)))
                                   row))
         (row-bytes           (+ nbcols (reduce #'+ col-bytes)))
         (connection          (cl-postgres::copier-database copier))
	 (cl-postgres::socket (cl-postgres::connection-socket connection)))
    (cl-postgres::with-reconnect-restart connection
      (cl-postgres::using-connection connection
        (cl-postgres::with-syncing
          (cl-postgres::write-uint1 cl-postgres::socket 100)
          (cl-postgres::write-uint4 cl-postgres::socket (+ 4 row-bytes))
          (macrolet ((send-byte (byte)
                       `(write-byte ,byte cl-postgres::socket)))
            (loop :for col :across row
               :for i fixnum :from 1
               :do (if (col-null-p col)
                       (progn
                         (send-byte #. (char-code #\\))
                         (send-byte #. (char-code #\N)))

                       (loop :for char :across col
                          :do (as-copy-utf-8-bytes char send-byte)))
               :do (if (< i nbcols)
                       (send-byte #. (char-code #\Tab))
                       (send-byte #. (char-code #\Newline))))))))
    (incf (cl-postgres::copier-count copier))
    row-bytes))


(defun db-write-escaped-vector-row (copier row &optional (nbcols (length row)))
  "Copy cl-postgres:db-write-row guts to avoid computing utf-8 bytes all
   over again, as we reproduced the data formating in pgloader code. The
   reason we do that is to be able to lower the cost of retrying batches:
   the formating has then already been done."
  (declare (optimize (speed 3) (space 0) (debug 1) (compilation-speed 0)))
  (let* ((col-bytes           (map 'vector
                                   (lambda (col)
                                     (if (col-null-p col) 2
                                         (utf-8-byte-length col)))
                                   row))
         (row-bytes           (+ nbcols (reduce #'+ col-bytes)))
         (connection          (cl-postgres::copier-database copier))
	 (cl-postgres::socket (cl-postgres::connection-socket connection)))
    (cl-postgres::with-reconnect-restart connection
      (cl-postgres::using-connection connection
        (cl-postgres::with-syncing
          (cl-postgres::write-uint1 cl-postgres::socket 100)
          (cl-postgres::write-uint4 cl-postgres::socket (+ 4 row-bytes))
          (macrolet ((send-byte (byte)
                       `(write-byte ,byte cl-postgres::socket)))
            (loop :for col :across row
               :for i fixnum :from 1
               :do (if (col-null-p col)
                       (progn
                         (send-byte #. (char-code #\\))
                         (send-byte #. (char-code #\N)))

                       (loop :for char :across col
                          :do (as-utf-8-bytes char send-byte)))
               :do (if (< i nbcols)
                       (send-byte #. (char-code #\Tab))
                       (send-byte #. (char-code #\Newline))))))))
    (incf (cl-postgres::copier-count copier))
    row-bytes))

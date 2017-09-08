;;;
;;; Implement bad row reject processing
;;;
(in-package :pgloader.monitor)

;;;
;;; When a batch has been refused by PostgreSQL with a data-exception, that
;;; means it contains non-conforming data. Log the error message in a log
;;; file and the erroneous data in a rejected data file for further
;;; processing.
;;;
(defun %process-bad-row (table condition row)
  "Add the row to the reject file, in PostgreSQL COPY TEXT format"
  (let* ((data  (pgtable-reject-data table))
         (logs  (pgtable-reject-logs table)))

    ;; first log the rejected data
    (with-open-file (reject-data-stream data
                                        :direction :output
                                        :element-type '(unsigned-byte 8)
                                        :if-exists :append
                                        :if-does-not-exist :create)
      ;; the row has already been processed when we get here
      (write-sequence row reject-data-stream)
      (write-byte #. (char-code #\Newline) reject-data-stream))

    ;; now log the condition signaled to reject the data
    (with-open-file (reject-log-stream logs
                                       :direction :output
                                       :if-exists :append
                                       :if-does-not-exist :create
                                       :external-format :utf-8)
      ;; the row has already been processed when we get here
      (format reject-log-stream "~a~%" condition))))

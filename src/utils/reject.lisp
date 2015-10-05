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
(defun %process-bad-row (table-name condition row)
  "Add the row to the reject file, in PostgreSQL COPY TEXT format"
  ;; first, update the stats.
  (let ((state (getf *sections* :data)))
    (pgstate-incf state table-name :errs 1)

    ;; now, the bad row processing
    (let* ((table (pgstate-get-label state table-name))
           (data  (pgtable-reject-data table))
           (logs  (pgtable-reject-logs table)))

      ;; first log the rejected data
      (with-open-file (reject-data-file data
                                        :direction :output
                                        :if-exists :append
                                        :if-does-not-exist :create
                                        :external-format :utf-8)
        ;; the row has already been processed when we get here
        (write-string row reject-data-file))

      ;; now log the condition signaled to reject the data
      (with-open-file (reject-logs-file logs
                                        :direction :output
                                        :if-exists :append
                                        :if-does-not-exist :create
                                        :external-format :utf-8)
        ;; the row has already been processed when we get here
        (format reject-logs-file "~a~%" condition)))))

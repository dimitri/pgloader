;;;
;;; The PostgreSQL COPY TO implementation, using S3 as an intermediate
;;; location for the data.
;;;
;;; This file is only used for Redshift support at the moment.
;;;
(in-package :pgloader.pgcopy)

(defun batch-rows-to-s3-then-copy (table columns copy nbcols queue)
  "Add rows that we pop from QUEUE into a batch, that we then COPY over to
   PostgreSQL as soon as the batch is full. This allows sophisticated error
   handling and recovery, where we can retry rows that are not rejected by
   PostgreSQL."
  (let ((seconds       0)
        (current-batch (make-batch)))
    (loop
       :for row := (lq:pop-queue queue)
       :until (eq :end-of-data row)
       :do (multiple-value-bind (maybe-new-batch seconds-in-this-batch)
               (add-row-to-current-batch table columns copy nbcols
                                         current-batch row
                                         :send-batch-fn #'send-batch-through-s3
                                         :format-row-fn #'prepare-and-format-row-for-s3)
             (setf current-batch maybe-new-batch)
             (incf seconds seconds-in-this-batch)))

    ;; the last batch might not be empty
    (unless (= 0 (batch-count current-batch))
      (incf seconds (send-batch-through-s3 table columns current-batch)))

    seconds))


(defun prepare-and-format-row-for-s3 (copy nbcols row)
  "Redshift doesn't know how to parse COPY format, we need to upload CSV
   instead. That said, we don't have to be as careful with the data layout
   and unicode representation when COPYing from a CSV file as we do when
   implementing the data streaming outselves."
  (declare (ignore copy nbcols))
  (let ((pg-vector-row (cl-csv:write-csv-row (coerce row 'list)
                                             :separator #\,
                                             :quote #\"
                                             :escape #(#\" #\")
                                             :newline #(#\Newline)
                                             :always-quote t)))
    (log-message :data "> ~s" pg-vector-row)
    (values pg-vector-row (length pg-vector-row))))


(defun send-batch-through-s3 (table columns batch &key (db pomo:*database*))
  "Copy current *writer-batch* into TABLE-NAME."
  (let ((batch-start-time (get-internal-real-time))
        (table-name       (format-table-name table))
        (pomo:*database*  db))

    ;;
    ;; We first upload the batch of data we have to S3
    ;;
    (multiple-value-bind (aws-access-key-id
                          aws-secret-access-key
                          aws-region
                          aws-s3-bucket)
        ;; TODO: implement --aws--profile and use it here
        (get-aws-credentials-and-setup)

      (let ((s3-filename (format nil "~a.~a.~a"
                                 (format-table-name table)
                                 (lp:kernel-worker-index)
                                 (batch-start batch)))
            (vector      (batch-as-single-vector batch)))

        (log-message :info
                     "Uploading a batch of ~a rows [~a] to s3://~a/~a"
                     (batch-count batch)
                     (pretty-print-bytes (batch-bytes batch))
                     aws-s3-bucket
                     s3-filename)

        (zs3:put-vector vector
                        aws-s3-bucket
                        s3-filename
                        :credentials (list aws-access-key-id
                                           aws-secret-access-key))

        ;; Now we COPY the data from S3 to Redshift:
        ;;
        ;; https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
        ;;
        (handler-case
            (with-pgsql-transaction (:database db)
              (let ((sql (format nil "COPY ~a FROM 's3://~a/~a' FORMAT CSV TIMEFORMAT 'auto' REGION '~a' ACCESS_KEY_ID '~a'"
                                 table-name
                                 aws-s3-bucket
                                 s3-filename
                                 aws-region
                                 aws-access-key-id)))
                (log-message :sql "~a" sql)
                (let ((sql-with-access-key
                       (format nil "~a
SECRET_ACCESS_KEY '~a'"
                               sql
                               aws-secret-access-key)))
                  (pomo:execute sql-with-access-key))))

          ;; If PostgreSQL signals a data error, process the batch by isolating
          ;; erroneous data away and retrying the rest.
          (postgresql-retryable (condition)
            (pomo:execute "ROLLBACK")
            (log-message :error "PostgreSQL [~s] ~a" table-name condition)
            (update-stats :data table :errs (batch-count batch)))

          (postgresql-unavailable (condition)
            (log-message :error "[PostgreSQL ~s] ~a" table-name condition)
            (log-message :error "Copy Batch reconnecting to PostgreSQL")

            ;; in order to avoid Socket error in "connect": ECONNREFUSED if we
            ;; try just too soon, wait a little
            (sleep 2)

            (cl-postgres:reopen-database db)
            (send-batch-through-s3 table columns batch :db db))

          (copy-init-error (condition)
            ;; Couldn't init the COPY protocol, process the condition up the
            ;; stack
            (update-stats :data table :errs 1)
            (error condition))

          (condition (c)
            ;; non retryable failures
            (log-message :error "Non-retryable error ~a" c)
            (pomo:execute "ROLLBACK")))))

    ;; now log about having send a batch, and update our stats with the
    ;; time that took
    (let ((seconds (elapsed-time-since batch-start-time)))
      (log-message :debug
                   "send-batch[~a] ~a ~d row~:p [~a] in ~6$s~@[ [oversized]~]"
                   (lp:kernel-worker-index)
                   (format-table-name table)
                   (batch-count batch)
                   (pretty-print-bytes (batch-bytes batch))
                   seconds
                   (batch-oversized-p batch))
      (update-stats :data table
                    :rows (batch-count batch)
                    :bytes (batch-bytes batch))

      ;; and return batch-seconds
      seconds)))


(defun batch-as-single-vector (batch)
  "For communicating with AWS S3, we finalize our batch data into a single
   vector."
  (if (= 0 (batch-count batch))
      nil

      ;; non-empty batch
      ;;
      ;; first compute the total number of bytes we need to represent this
      ;; batch, and then flatten it into a single vector of that size.
      ;;
      ;; So now we now how many bytes we need to finalize this batch
      ;;
      (let* ((bytes  (batch-bytes batch))
             (vector (make-array bytes :element-type 'character)))
        (loop :for count :below (batch-count batch)
           :for pos := 0 :then (+ pos (length row))
           :for row :across (batch-data batch)
           :do (when row
                 (replace vector row :start1 pos)))

        vector)))

;;;
;;; S3 support needs some AWS specific setup. We use the same configuration
;;; files as the main AWS command line interface, as documented at the
;;; following places:
;;;
;;;  https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html
;;;  https://docs.aws.amazon.com/cli/latest/userguide/cli-multiple-profiles.html
;;;  https://docs.aws.amazon.com/cli/latest/userguide/cli-environment.html
;;;
(defun get-aws-credentials-and-setup (&optional profile)
  "Returns AWS access key id, secret access key, region and S3 bucket-name
   from environment or ~/.aws/ configuration files, as multiple values."
  (let* (aws-access-key-id
         aws-secret-access-key
         aws-region
         aws-s3-bucket-name
         (aws-directory (uiop:native-namestring
                         (uiop:merge-pathnames* ".aws/"
                                                (user-homedir-pathname))))
         (aws-config-fn (make-pathname :name "config"
                                       :directory aws-directory))
         (aws-creds-fn  (make-pathname :name "credentials"
                                       :directory aws-directory))
         (aws-config    (ini:make-config))
         (credentials   (ini:make-config))

         (conf-profile  (if profile (format nil "profile ~a" profile)
                            "default"))
         (creds-profile (or profile "default")))

    ;; read config files
    (ini:read-files aws-config (list aws-config-fn))
    (ini:read-files credentials (list aws-creds-fn))

    ;; get values from the environment, and if not in the env, from the
    ;; configuration files.
    (setf aws-access-key-id
          (or (uiop:getenv "AWS_ACCESS_KEY_ID")
              (ini:get-option credentials creds-profile "aws_access_key_id")))

    (setf aws-secret-access-key
          (or (uiop:getenv "AWS_SECRET_ACCESS_KEY")
              (ini:get-option credentials creds-profile "aws_secret_access_key")))

    (setf aws-region
          (or (uiop:getenv "AWS_DEFAULT_REGION")
              (ini:get-option aws-config conf-profile "region")))

    (setf aws-s3-bucket-name (or (uiop:getenv "AWS_S3_BUCKET_NAME")
                                 "pgloader"))

    (values aws-access-key-id
            aws-secret-access-key
            aws-region
            aws-s3-bucket-name)))

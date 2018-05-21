;;;
;;; The PostgreSQL COPY TO implementation, with batches and retries.
;;;
(in-package :pgloader.pgcopy)

;;;
;;; We receive raw input rows from an lparallel queue, push their content
;;; down to PostgreSQL, handling any data related errors in the way.
;;;
(defun copy-rows-from-queue (copy queue
                             &key
                               disable-triggers
                               on-error-stop
                               (columns
                                (pgloader.sources:copy-column-list copy))
                             &aux
                               (pgconn  (clone-connection
                                         (pgloader.sources:target-db copy)))
                               (table   (pgloader.sources:target copy)))
  "Fetch rows from the QUEUE, prepare them in batches and send them down to
   PostgreSQL, and when that's done update stats."
  (let* ((nbcols        (length
                         (table-column-list (pgloader.sources::target copy))))
         (seconds 0))

    ;; we need to compute some information and have them at the right place
    ;; FIXME: review the API here, that's an half-baked refactoring.
    (prepare-copy-parameters copy)

    (log-message :info "COPY ON ERROR ~:[RESUME NEXT~;STOP~]" on-error-stop)

    (pgloader.pgsql:with-pgsql-connection (pgconn)
      (with-schema (unqualified-table-name table)
        (with-disabled-triggers (unqualified-table-name
                                 :disable-triggers disable-triggers)
          (log-message :info "pgsql:copy-rows-from-queue[~a]: ~a ~a"
                       (lp:kernel-worker-index)
                       (format-table-name table)
                       columns)

          (let ((copy-fun
                 (cond ((eq :redshift (pgconn-variant pgconn))
                        ;;
                        ;; When using Redshift as the target, we lose the
                        ;; COPY FROM STDIN feature, and we have to use S3 as
                        ;; an intermediate step. We then upload content a
                        ;; batch at a time, and don't follow the
                        ;; on-error-stop setting.
                        ;;
                        (log-message :log "copy-rows-from-queue REDSHIFT")
                        (function batch-rows-to-s3-then-copy))

                       (on-error-stop
                        ;;
                        ;; When on-error-stop is true, we don't need to
                        ;; handle batch processing, we can stop as soon as
                        ;; there's a failure.
                        ;;
                        (function stream-rows-to-copy))

                       (t
                        ;;
                        ;; When on-error-stop is nil, we actually implement
                        ;; on-error-resume-next behavior, and for that we
                        ;; need to keep a batch of rows around in order to
                        ;; replay COPYing its content around, skipping rows
                        ;; that are rejected by PostgreSQL.
                        ;;
                        (function batch-rows-to-copy)))))

            ;;
            ;; As all our function have the same API. we can just funcall
            ;; the selected one here.
            ;;
            (incf seconds
                  (funcall copy-fun table columns copy nbcols queue))))))

    ;; each writer thread sends its own stop timestamp and the monitor keeps
    ;; only the latest entry
    (update-stats :data table :ws seconds :stop (get-internal-real-time))
    (log-message :debug "Writer[~a] for ~a is done in ~6$s"
                 (lp:kernel-worker-index)
                 (format-table-name table)
                 seconds)
    (list :writer table seconds)))

(defun prepare-copy-parameters (copy)
  "add some COPY activity related bits to our COPY object."
  (setf (transforms copy)
        (let ((funs (transforms copy)))
          (unless (every #'null funs)
            funs))

        ;; FIXME: we should change the API around preprocess-row, someday.
        (preprocessor copy)
        (pgloader.sources::preprocess-row copy)

        ;; FIXME: we could change the API around data-is-preformatted-p,
        ;; but that's a bigger change than duplicating the information in
        ;; the object.
        (copy-format copy)
        (if (data-is-preformatted-p copy) :escaped :raw)))


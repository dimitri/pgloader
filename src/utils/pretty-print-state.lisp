;;;
;;; Pretty print a report while doing bulk operations
;;;

(in-package :pgloader.state)

;;
;; define a tree of print format so that we can generalize some methods on
;; any “data” type format or any “human readable” print format.
;;
(defstruct print-format)

(defstruct (print-format-human-readable (:include print-format)
                                        (:conc-name pf-))
  (legend           nil :type (or null string))
  (max-label-length   1 :type fixnum))

(defstruct (print-format-text    (:include print-format-human-readable)))
(defstruct (print-format-verbose (:include print-format-human-readable)))

(defstruct (print-format-tab  (:include print-format)))
(defstruct (print-format-csv  (:include print-format-tab)))
(defstruct (print-format-copy (:include print-format-tab)))

(defstruct (print-format-json (:include print-format)))

(defgeneric pretty-print (stream state format &key))


;;;
;;; The pretty-print methods for data and human readable formats for a
;;; global state are a “shell” that calls into the more specialized version,
;;; for pgstate then pgtable.
;;;
(defmethod pretty-print ((stream stream)
                         (state state)
                         (format print-format-tab)
                         &key)
  (loop :for pgstate :in (list (state-preload state)
                               (state-data state)
                               (state-postload state))
     :do (pretty-print stream pgstate format)))

(defmethod pretty-print ((stream stream)
                         (state state)
                         (format print-format-json)
                         &key)
  (yason:encode state stream))

(defmethod pretty-print ((stream stream)
                         (state state)
                         (format print-format-human-readable)
                         &key)
  (when (and (state-preload state) (pgstate-tabnames (state-preload state)))
    (pretty-print *report-stream*
                  (state-preload state)
                  format
                  :header t
                  :footer nil
                  :extra-sep-line t))

  (pretty-print *report-stream*
                (state-data state)
                format
                :header (not (and (state-preload state)
                                  (pgstate-tabnames (state-preload state))))
                :footer (unless (and (state-postload state)
                                     (pgstate-tabnames (state-postload state)))
                          (pf-legend format))
                :extra-sep-line t)

  (when (and (state-postload state) (pgstate-tabnames (state-postload state)))
    (pretty-print *report-stream*
                  (state-postload state)
                  format
                  :header nil
                  :footer (pf-legend format))))


;;;
;;; Support for TEXT format, human readable
;;;
(defmethod pretty-print ((stream stream)
                         (pgstate pgstate)
                         (format print-format-text)
                         &key
                           header
                           footer
                           extra-sep-line)
  (let* ((max-len      (pf-max-label-length format))
         (col-label    (make-string max-len :initial-element #\-))
         (col-sep      (make-string 9 :initial-element #\-))
         (col-long-sep (make-string 14 :initial-element #\-))
         (sep-line     (format nil
                               "~&~v@a  ~9@a  ~9@a  ~9@a  ~14@a~%"
                               max-len
                               col-label col-sep col-sep col-sep col-long-sep)))
    (when header
      (format stream
              "~&~v@a  ~9@a  ~9@a  ~9@a  ~14@a~%"
              max-len
              "table name" "errors" "rows" "bytes" "total time")
      (format stream sep-line))

    (loop
       :for label :in (reverse (pgstate-tabnames pgstate))
       :for pgtable := (gethash label (pgstate-tables pgstate))
       :do (let ((legend (etypecase label
                           (string label)
                           (table  (format-table-name label)))))
             (pretty-print stream pgtable format
                           :legend legend
                           :max-label-length max-len)))

    (cond (extra-sep-line
           (format stream sep-line))

          (footer
           (with-slots (tabnames errs rows bytes secs) pgstate
             (when tabnames
               (format stream sep-line)
               (format stream
                       "~&~v@a  ~9@a  ~9@a  ~9@a  ~14@a~%"
                       max-len footer
                       (if (zerop errs) "✓" errs)
                       rows
                       (if (zerop bytes) ""
                           (pgloader.utils::pretty-print-bytes bytes))
                       (format-interval secs nil))))))))

(defmethod pretty-print ((stream stream)
                         (pgtable pgtable)
                         (format print-format-text)
                         &key legend max-label-length)
  (with-slots (errs rows bytes secs rs ws) pgtable
    (format stream
            "~&~v@a  ~9@a  ~9@a  ~9@a  ~14@a~%"
            max-label-length
            legend
            errs
            rows
            (if (zerop bytes) ""
                (pgloader.utils::pretty-print-bytes bytes))
            (format-interval-guess secs rs ws))))


;;;
;;; Support for VERBOSE format, human readable
;;;
(defmethod pretty-print ((stream stream)
                         (pgstate pgstate)
                         (format print-format-verbose)
                         &key
                           header
                           footer
                           extra-sep-line)
  (let* ((max-len      (pf-max-label-length format))
         (col-label    (make-string max-len :initial-element #\-))
         (col-sep      (make-string 9 :initial-element #\-))
         (col-long-sep (make-string 14 :initial-element #\-))
         (sep-line
          (format nil
                  "~&~v@a  ~9@a  ~9@a  ~9@a  ~9@a  ~14@a  ~9@a  ~9@a~%"
                  max-len col-label
                  col-sep col-sep col-sep col-sep
                  col-long-sep col-sep col-sep)))
    (when header
      (format stream
              "~&~v@a  ~9@a  ~9@a  ~9@a  ~9@a  ~14@a  ~9@a  ~9@a~%"
              max-len
              "table name" "errors" "read" "imported" "bytes" "total time" "read" "write")
      (format stream sep-line))

    (loop
       :for label :in (reverse (pgstate-tabnames pgstate))
       :for pgtable := (gethash label (pgstate-tables pgstate))
       :do (let ((legend (etypecase label
                           (string label)
                           (table  (format-table-name label)))))
             (pretty-print stream pgtable format
                           :legend legend
                           :max-label-length max-len)))

    (cond (extra-sep-line
           (format stream sep-line))

          (footer
           (with-slots (tabnames read rows errs bytes secs rs ws) pgstate
             (when tabnames
               (format stream sep-line)
               (format stream
                       "~&~v@a  ~9@a  ~9@a  ~9@a  ~9@a  ~14@a  ~@[~9@a~]  ~@[~9@a~]~%"
                       max-len
                       footer
                       (if (zerop errs) "✓" errs)
                       read
                       rows
                       (if (zerop bytes) ""
                           (pgloader.utils::pretty-print-bytes bytes))
                       (format-interval secs nil)
                       (when (and rs (/= rs 0.0))
                         (format-interval rs nil))
                       (when (and ws (/= ws 0.0))
                         (format-interval ws nil)))))))))

(defmethod pretty-print ((stream stream)
                         (pgtable pgtable)
                         (format print-format-verbose)
                         &key legend max-label-length)
  (with-slots (read rows errs bytes secs rs ws) pgtable
    (format stream
            "~&~v@a  ~9@a  ~9@a  ~9@a  ~9@a  ~14@a  ~@[~9@a~]  ~@[~9@a~]~%"
            max-label-length
            legend
            errs
            read
            rows
            (if (zerop bytes) ""
                (pgloader.utils::pretty-print-bytes bytes))
            (format-interval-guess secs rs ws)
            (when (and rs (/= rs 0.0)) (format-interval rs nil))
            (when (and ws (/= ws 0.0)) (format-interval ws nil)))))


;;;
;;; Support for CSV format
;;;
(defmethod pretty-print ((stream stream)
                         (pgstate pgstate)
                         (format print-format-csv)
                         &key)
  (loop
     :for label :in (reverse (pgstate-tabnames pgstate))
     :for pgtable := (gethash label (pgstate-tables pgstate))
     :do (let ((legend (etypecase label
                         (string label)
                         (table  (format-table-name label)))))
           (pretty-print stream pgtable format :legend legend))))

(defmethod pretty-print ((stream stream)
                         (pgtable pgtable)
                         (format print-format-csv)
                         &key legend)
  (with-slots (read rows errs bytes secs rs ws) pgtable
    (format stream
            "~&~s;~s;~s;~s;~s;~s"
            legend
            read
            rows
            errs
            bytes
            (format-interval-guess secs rs ws))))


;;;
;;; Support for COPY format
;;;
(defmethod pretty-print ((stream stream)
                         (pgstate pgstate)
                         (format print-format-copy)
                         &key)
  (loop
     :for label :in (reverse (pgstate-tabnames pgstate))
     :for pgtable := (gethash label (pgstate-tables pgstate))
     :do (let ((legend (etypecase label
                         (string label)
                         (table  (format-table-name label)))))
           (pretty-print stream pgtable format :legend legend))))

(defmethod pretty-print ((stream stream)
                         (pgtable pgtable)
                         (format print-format-copy)
                         &key legend)
  (with-slots (read rows errs bytes secs rs ws) pgtable
    (format stream
            "~&~a	~s	~s	~s	~s	~s"
            legend
            read
            rows
            errs
            bytes
            (format-interval-guess secs rs ws))))


;;;
;;; In the main output format, guess what's the real time spent when we
;;; don't have it.
;;;
(defun format-interval-guess (secs rs ws)
  (cond ((> 0 secs)                 (format-interval secs nil))
        ((and rs ws (= 0 secs))     (format-interval (max rs ws) nil))
        (t                          (format-interval secs nil))))


;;;
;;; Have yason output JSON formated output, straight from our instances.
;;;
(defmacro define-yason-encoder (class)
  "Define a new yason:encode method for CLASS."
  `(defmethod yason:encode ((instance ,class)
                            &optional (stream *standard-output*))
     "Encode an EXTENSION object into JSON."
     (yason:with-output (stream)
       (yason:with-object ()
         (loop :for slot :in (closer-mop:compute-slots ,class)
            :for slot-name := (closer-mop:slot-definition-name slot)
            :do (unless (member slot-name '(reject-data reject-logs))
                  (yason:encode-object-element
                   (string slot-name)
                   (slot-value instance slot-name))))))))

(define-yason-encoder #. (find-class 'state))
(define-yason-encoder #. (find-class 'pgtable))

(defmethod yason:encode ((pgstate pgstate) &optional (stream *standard-output*))
  "Output catalog tables by name only, don't recurse into the catalog..."
  (yason:with-output (stream)
    (yason:with-array ()
      (yason:encode-array-element
       (loop :for label :in (reverse (pgstate-tabnames pgstate))
          :for pgtable := (gethash label (pgstate-tables pgstate))
          :collect pgtable)))))

(defmethod yason:encode ((table table) &optional (stream *standard-output*))
  (yason:with-output (stream)
    (yason:encode (format-table-name table))))

;;;
;;; Common methods implementations, default behaviour, for md objects
;;;

(in-package #:pgloader.sources)

(defmethod parse-header ((copy md-copy))
  "Unsupported by default, to be implemented in each md-copy subclass."
  (error "Parsing the header of a ~s is not implemented yet." (type-of copy)))

(defmethod preprocess-row ((copy md-copy))
  "The file based readers possibly have extra work to do with user defined
   fields to columns projections (mapping)."
  (reformat-then-process :fields  (fields copy)
                         :columns (columns copy)
                         :target  (target copy)))

(defmethod copy-column-list ((copy md-copy))
  "We did reformat-then-process the column list, so we now send them in the
   COPY buffer as found in (columns fixed)."
  (mapcar (lambda (col)
            ;; always double quote column names
            (format nil "~s" (car col)))
          (columns copy)))

(defmethod clone-copy-for ((copy md-copy) path-spec)
  "Create a copy of CSV for loading data from PATH-SPEC."
  (make-instance (class-of copy)
                 ;; source-db is expected unbound here, so bypassed
                 :target-db  (clone-connection (target-db copy))
                 :source     (make-instance (class-of (source copy))
                                            :spec (md-spec (source copy))
                                            :type (conn-type (source copy))
                                            :path path-spec)
                 :target     (target copy)
                 :fields     (fields copy)
                 :columns    (columns copy)
                 :transforms (or (transforms copy)
                                 (make-list (length (columns copy))))
                 :encoding   (encoding copy)
                 :skip-lines (skip-lines copy)
                 :header     (header copy)))

(defmethod map-rows ((copy md-copy) &key process-row-fn)
  "Load data from a text file in CSV format, with support for advanced
   projecting capabilities. See `project-fields' for details.

   Each row is pre-processed then PROCESS-ROW-FN is called with the row as a
   list as its only parameter.

   Finally returns how many rows where read and processed."

  (with-connection (cnx (source copy)
                        :direction :input
                        :external-format (encoding copy)
                        :if-does-not-exist nil)
    (let ((input (md-strm cnx)))
     ;; we handle skipping more than one line here, as cl-copy only knows
     ;; about skipping the first line
      (loop :repeat (skip-lines copy) :do (read-line input nil nil))

      ;; we might now have to skip the header line
      (when (header copy) (read-line input nil nil))

      ;; read in the text file, split it into columns
      (process-rows copy input process-row-fn))))

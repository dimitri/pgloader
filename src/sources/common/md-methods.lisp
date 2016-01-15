;;;
;;; Generic API for pgloader sources
;;; Methods for source types with multiple files input
;;;

(in-package :pgloader.sources)

(defmethod parse-header ((copy md-copy) header)
  "Unsupported by default, to be implemented in each md-copy subclass."
  (error "Parsing the header of a ~s is not implemented yet." (type-of copy)))

(defmethod map-rows ((copy md-copy) &key process-row-fn)
  "Load data from a text file in CSV format, with support for advanced
   projecting capabilities. See `project-fields' for details.

   Each row is pre-processed then PROCESS-ROW-FN is called with the row as a
   list as its only parameter.

   Finally returns how many rows where read and processed."

  (with-connection (cnx (source copy))
    (loop :for input := (open-next-stream cnx
                                          :direction :input
                                          :external-format (encoding copy)
                                          :if-does-not-exist nil)
       :while input
       :do (progn
             ;; we handle skipping more than one line here, as cl-copy only knows
	     ;; about skipping the first line
	     (loop repeat (skip-lines copy) do (read-line input nil nil))

             ;; we might now have to read the fields from the header line
             (when (header copy)
               (setf (fields copy)
                     (parse-header copy (read-line input nil nil)))

               (log-message :debug "Parsed header columns ~s" (fields copy)))

	     ;; read in the text file, split it into columns
	     (process-rows copy input process-row-fn)))))

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

(defmethod copy-database ((copy md-copy)
                          &key
                            truncate
                            disable-triggers
			    drop-indexes

                            ;; generic API, but ignored here
			    data-only
			    schema-only
                            create-tables
			    include-drop
			    create-indexes
			    reset-sequences)
  "Copy the contents of the COPY formated file to PostgreSQL."
  (declare (ignore data-only schema-only
                   create-tables include-drop
                   create-indexes reset-sequences))

  ;; this sets (table-index-list (target copy))
  (maybe-drop-indexes (target-db copy)
                      (target copy)
                      :drop-indexes drop-indexes)

  (copy-from copy :truncate truncate :disable-triggers disable-triggers)

  ;; re-create the indexes from the target table entry
  (create-indexes-again (target-db copy)
                        (target copy)
                        :drop-indexes drop-indexes))

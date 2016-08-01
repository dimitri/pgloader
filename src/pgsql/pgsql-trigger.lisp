;;;
;;; Create the PostgreSQL schema from our internal Catalog representation.
;;; Here, triggers and their stored procedures.
;;;

(in-package #:pgloader.pgsql)

(defvar *pgsql-triggers-procedures*
  `((:on-update-current-timestamp .
     ,(lambda (trigger column table)
              (let ((body (format nil
                                  "BEGIN~%  NEW.~a = now();~%  RETURN NEW;~%END;"
                                  (column-name column))))
                (make-procedure :name (trigger-procedure-name trigger)
                                :returns "trigger"
                                :language "plpgsql"
                                :body body)))))
  "List of lambdas to generate procedure definitions from pgloader internal
   trigger names as positioned in the internal catalogs at CAST time.")

(defun rename-trigger (trigger)
  "Turn a common lisp symbol into a proper PostgreSQL trigger name."
  (setf (trigger-name trigger)
        (string-downcase
         (cl-ppcre:regex-replace-all "-"
                                     (symbol-name (trigger-name trigger))
                                     "_"))))

(defun process-triggers (table)
  "Return the list of PostgreSQL statements to create a catalog trigger."
  (loop :for column :in (table-column-list table)
     :when (column-extra column)
     :do (etypecase (column-extra column)
           (trigger
            ;; finish the trigger CAST and attach it to the table now
            (let* ((trigger (column-extra column))
                   (proc    (or (trigger-procedure trigger)
                                ;;
                                ;; We have a trigger with no attached
                                ;; procedure, so we search for the trigger
                                ;; procedure-name in
                                ;; *pgsql-triggers-procedures* to find a
                                ;; lambda form to call to produce our PLpgSQL
                                ;; procedure
                                ;;
                                (let ((generate-proc
                                       (cdr
                                        (assoc (trigger-name trigger)
                                               *pgsql-triggers-procedures*))))
                                  (assert (functionp generate-proc))
                                  (funcall generate-proc
                                           trigger column table)))))
              ;;
              ;; Properly attach the procedure to the trigger and the
              ;; trigger to the table.
              ;;
              (unless (trigger-procedure trigger)
                (setf (trigger-procedure trigger) proc))

              (rename-trigger trigger)

              (setf (column-extra column) nil)

              (setf (trigger-table trigger) table)
              (push-to-end trigger (table-trigger-list table)))))))

;;;
;;; Create the PostgreSQL schema from our internal Catalog representation.
;;; Here, triggers and their stored procedures.
;;;

(in-package #:pgloader.pgsql)

(defvar *pgsql-triggers-procedures*
  `((:on-update-current-timestamp
     .
     ,(lambda (schema proc-name column-list)
        (let ((body (format nil
                            "~
BEGIN
   ~{NEW.~a = now();~^~%   ~}
   RETURN NEW;
END;"
                            (mapcar #'column-name column-list))))
          (make-procedure :schema schema
                          :name proc-name
                          :returns "trigger"
                          :language "plpgsql"
                          :body body)))))
  "List of lambdas to generate procedure definitions from pgloader internal
   trigger names as positioned in the internal catalogs at CAST time.")

(defun build-trigger (trigger-symbol-name table column-list action)
  "Take a synthetic TRIGGER as generated from per-source cast methods and
   complete it into a proper trigger, attached on TABLE, firing on ACTION,
   impacting COLUMN-LIST."
  (let* ((tg-name   (string-downcase
                     (cl-ppcre:regex-replace-all
                      "-" (symbol-name trigger-symbol-name) "_")))
         (proc-name (build-identifier "_" tg-name (table-name table)))
         (gen-proc  (cdr
                     (assoc trigger-symbol-name *pgsql-triggers-procedures*)))
         (schema    (schema-name (table-schema table)))
         (proc      (funcall gen-proc schema proc-name column-list)))

    ;;
    ;; Build our trigger definition now: real name, action, and procedure
    ;;
    (make-trigger :name tg-name
                  :table table
                  :action action
                  :procedure proc)))

(defun process-triggers (table)
  "Return the list of PostgreSQL statements to create a catalog trigger."
  (let ((triggers-by-name (make-hash-table)))
    ;;
    ;; trigger names at this stage are normalized to
    ;; *pgsql-triggers-procedures* keys, like :on-update-current-timestamp
    ;; our job is to transform them into proper trigger definitions
    ;;
    ;; note that we might have several on update column definitions on the
    ;; same table, we want a single trigger that takes care of them all.
    ;;
    (loop :for column :in (table-column-list table)
       :do (when (trigger-p (column-extra column))
             (let* ((trigger (column-extra column))
                    (tg-name (trigger-name trigger)))
               (push column (gethash tg-name triggers-by-name)))))

    ;;
    ;; Now that we have a hash-table of column-list per trigger-name, build
    ;; the real triggers and attach them to our table.
    ;;
    (loop :for tg-name :being :the :hash-keys :of triggers-by-name
       :using (hash-value column-list)
       :do (ecase tg-name
             (:on-update-current-timestamp
              (let ((trigger
                     (build-trigger tg-name table column-list "BEFORE UPDATE")))
                (push-to-end trigger (table-trigger-list table))))))))

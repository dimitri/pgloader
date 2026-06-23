;;;
;;; Parse MySQL option files (~/.my.cnf, /etc/my.cnf, /etc/mysql/my.cnf)
;;; using the py-configparser library (already a pgloader dependency).
;;;
;;; Section priority: [pgloader] overrides [client], mirroring MySQL's
;;; program-name convention.  Search path mirrors the MySQL manual:
;;;   https://dev.mysql.com/doc/refman/8.0/en/option-files.html
;;;

(in-package :pgloader.source.mysql)

(defparameter *my-cnf-search-paths*
  (list "/etc/my.cnf" "/etc/mysql/my.cnf")
  "Option files read in order; later files override earlier ones.")

(defun my-cnf-home-path ()
  "Return the path string for ~/.my.cnf."
  (namestring (merge-pathnames ".my.cnf" (user-homedir-pathname))))

(defun read-my-cnf-config ()
  "Read all MySQL option files into a py-configparser config object.
   ini:read-files skips files that don't exist."
  (let ((config (ini:make-config))
        (paths  (append *my-cnf-search-paths*
                        (list (my-cnf-home-path)))))
    (ini:read-files config paths)
    config))

(defun cnf-get (config section key)
  "Return the value of KEY in SECTION, or nil if absent."
  (when (and (ini:has-section-p config section)
             (ini:has-option-p  config section key))
    (ini:get-option config section key)))

(defun extract-conn-params (config section)
  "Extract connection params from SECTION of CONFIG.
   Returns a plist of non-nil values only."
  (let ((host     (cnf-get config section "host"))
        (port-str (cnf-get config section "port"))
        (user     (cnf-get config section "user"))
        (password (cnf-get config section "password"))
        (dbname   (cnf-get config section "database")))
    (list :host     host
          :port     (when port-str (parse-integer port-str))
          :user     user
          :password password
          :dbname   dbname)))

(defun read-my-cnf ()
  "Read MySQL option files and return a plist of connection parameters.
   [client] values are the base; [pgloader] values override them.
   Keys with no value in either section are nil."
  (let* ((config   (read-my-cnf-config))
         (client   (extract-conn-params config "client"))
         (pgloader (extract-conn-params config "pgloader")))
    ;; merge: pgloader wins, fall back to client, nil stays nil
    (loop with result = (copy-list client)
          for (k v) on pgloader by #'cddr
          when v do (setf (getf result k) v)
          finally (return result))))

(defun apply-my-cnf (user password host port dbname)
  "Fill nil connection slots from MySQL option files.
   Returns (values user password host port dbname)."
  (let ((opts (read-my-cnf)))
    (values (or user     (getf opts :user))
            (or password (getf opts :password))
            (or host     (getf opts :host))
            (or port     (getf opts :port))
            (or dbname   (getf opts :dbname)))))

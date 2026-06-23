;;;
;;; Parse MySQL option files (~/.my.cnf, /etc/my.cnf, /etc/mysql/my.cnf).
;;;
;;; Implements the section-lookup convention described in the MySQL manual:
;;;   https://dev.mysql.com/doc/refman/8.0/en/option-files.html
;;;
;;; [client]   — read by all MySQL clients; supplies base values.
;;; [pgloader] — pgloader-specific overrides; takes precedence over [client].
;;;
;;; Only connection-relevant keys are extracted: host, port, user, password,
;;; database.  Boolean options, socket, and !include/!includedir directives
;;; are intentionally ignored.
;;;

(in-package :pgloader.source.mysql)

(defparameter *my-cnf-search-paths*
  (list #p"/etc/my.cnf"
        #p"/etc/mysql/my.cnf")
  "Option files read in order; later files override earlier ones.")

(defun my-cnf-home-path ()
  "Return the path to ~/.my.cnf."
  (merge-pathnames #p".my.cnf" (user-homedir-pathname)))

(defun strip-quotes (s)
  "Remove surrounding single or double quotes from S."
  (let ((len (length s)))
    (if (and (>= len 2)
             (or (and (char= (char s 0) #\")
                      (char= (char s (1- len)) #\"))
                 (and (char= (char s 0) #\')
                      (char= (char s (1- len)) #\'))))
        (subseq s 1 (1- len))
        s)))

(defun parse-my-cnf-file (path)
  "Parse a single option file at PATH.
   Returns an alist of (section-name . ((key . value) ...)).
   Unknown sections are silently collected."
  (when (probe-file path)
    (with-open-file (stream path :direction :input :external-format :utf-8
                                 :if-does-not-exist nil)
      (when stream
        (let ((sections '())
              (current-section nil))
          (loop for line = (read-line stream nil nil)
                while line
                do (let ((trimmed (string-trim '(#\Space #\Tab) line)))
                     (cond
                       ;; blank lines and comments
                       ((or (zerop (length trimmed))
                            (char= (char trimmed 0) #\#)
                            (char= (char trimmed 0) #\;)
                            (char= (char trimmed 0) #\!))
                        nil)

                       ;; section header [name]
                       ((and (char= (char trimmed 0) #\[)
                             (char= (char trimmed (1- (length trimmed))) #\]))
                        (let ((name (subseq trimmed 1 (1- (length trimmed)))))
                          (setf current-section name)
                          (unless (assoc name sections :test #'string=)
                            (push (cons name '()) sections))))

                       ;; key=value pair
                       ((and current-section
                             (find #\= trimmed))
                        (let* ((eq-pos (position #\= trimmed))
                               (k (string-trim '(#\Space #\Tab)
                                               (subseq trimmed 0 eq-pos)))
                               (v (strip-quotes
                                   (string-trim '(#\Space #\Tab)
                                                (subseq trimmed (1+ eq-pos))))))
                          (let ((sec (assoc current-section sections
                                            :test #'string=)))
                            (unless (assoc k (cdr sec) :test #'string=)
                              (push (cons k v) (cdr sec)))))))))
          sections)))))

(defun merge-my-cnf-files (paths)
  "Merge option files in order; later files override earlier keys per section."
  (let ((result '()))
    (dolist (path paths result)
      (let ((file-sections (parse-my-cnf-file path)))
        (dolist (sec file-sections)
          (let ((name (car sec))
                (pairs (cdr sec)))
            (let ((existing (assoc name result :test #'string=)))
              (if existing
                  ;; merge: later file wins per key
                  (dolist (pair pairs)
                    (unless (assoc (car pair) (cdr existing) :test #'string=)
                      (push pair (cdr existing))))
                  (push (cons name (copy-list pairs)) result)))))))))

(defun section-conn-params (section-alist key)
  "Extract connection params from a section alist under KEY."
  (let ((sec (cdr (assoc key section-alist :test #'string=))))
    (when sec
      (list :host     (cdr (assoc "host"     sec :test #'string=))
            :port     (let ((p (cdr (assoc "port" sec :test #'string=))))
                        (when p (parse-integer p)))
            :user     (cdr (assoc "user"     sec :test #'string=))
            :password (cdr (assoc "password" sec :test #'string=))
            :dbname   (cdr (assoc "database" sec :test #'string=))))))

(defun read-my-cnf ()
  "Read MySQL option files and return a merged plist of connection parameters.
   Applies [client] first, then [pgloader] on top (higher priority).
   Returns nil when no option files exist or no relevant sections are found."
  (let* ((all-paths (append *my-cnf-search-paths*
                            (list (my-cnf-home-path))))
         (sections  (merge-my-cnf-files all-paths))
         (client    (section-conn-params sections "client"))
         (pgloader  (section-conn-params sections "pgloader")))
    ;; merge: pgloader values override client values, nils don't override
    (loop with result = (copy-list client)
          for (k v) on pgloader by #'cddr
          when v do (setf (getf result k) v)
          finally (return result))))

(defun apply-my-cnf (user password host port dbname)
  "Fill in nil connection slots from MySQL option files.
   Returns (values user password host port dbname) with any nil slots
   replaced by values from [client] or [pgloader] sections."
  (let ((opts (read-my-cnf)))
    (values (or user     (getf opts :user))
            (or password (getf opts :password))
            (or host     (getf opts :host))
            (or port     (getf opts :port))
            (or dbname   (getf opts :dbname)))))

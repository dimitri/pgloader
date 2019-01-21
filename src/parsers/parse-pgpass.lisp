;;;
;;; PostgreSQL pgpass parser, see
;;;  https://www.postgresql.org/docs/current/static/libpq-pgpass.html
;;;

(in-package :pgloader.parser)

(defstruct pgpass
  hostname port database username password)

(defun pgpass-char-p (char)
  (not (member char '(#\: #\\) :test #'char=)))

(defrule pgpass-escaped-char (and #\\ (or #\\ #\:))
  (:lambda (c) (second c)))

(defrule pgpass-ipv6-hostname (and #\[
                                   (+ (or (digit-char-p character) ":"))
                                   #\])
  (:lambda (ipv6) (text (second ipv6))))

(defrule pgpass-entry (or "*"
                          (+ (or pgpass-ipv6-hostname
                                 pgpass-escaped-char
                                 (pgpass-char-p character))))
  (:lambda (e) (text e)))

(defrule pgpass-line (and (? pgpass-entry) #\: pgpass-entry #\:
                          pgpass-entry #\: pgpass-entry #\:
                          (? pgpass-entry))
  (:lambda (pl)
    (make-pgpass :hostname (or (first pl) "localhost")
                 :port (third pl)
                 :database (fifth pl)
                 :username (seventh pl)
                 :password (ninth pl))))

(defun get-pgpass-filename ()
  "Return where to find .pgpass file"
  (or (uiop:getenv "PGPASSFILE")
      #-windows (uiop:merge-pathnames* (uiop:make-pathname* :name ".pgpass")
                                       (user-homedir-pathname))
      #+windows (let ((pgpass-dir (format nil "~a/~a/"
                                          (uiop:getenv " %APPDATA%")
                                          "postgresql")))
                  (uiop:make-pathname* :directory pgpass-dir
                                       :name "pgpass"
                                       :type "conf"))))

(defun parse-pgpass-file (&optional pgpass-filename)
  (let ((pgpass-filename (or pgpass-filename (get-pgpass-filename))))
    (when (and pgpass-filename (probe-file pgpass-filename))
      (with-open-file (s pgpass-filename
                         :direction :input
                         :if-does-not-exist nil
                         :element-type 'character)
        (when s
          (loop :for line := (read-line s nil nil)
             :while line
             :when (and line
                        (< 0 (length line))
                        (char/= #\# (aref line 0)))
             :collect (parse 'pgpass-line line)))))))

(defun match-hostname (pgpass hostname)
  "A host name of localhost matches both TCP (host name localhost) and Unix
  domain socket (pghost empty or the default socket directory) connections
  coming from the local machine."
  (cond ((and (string= "localhost" (pgpass-hostname pgpass))
              (or (eq :unix hostname)
                  (and (stringp hostname)
                       (string= "localhost" hostname)))))
        ((string= "*" (pgpass-hostname pgpass))
         t)
        (t
         (and (stringp hostname)
              (string= (pgpass-hostname pgpass) hostname)))))

(defun match-pgpass (pgpass hostname port database username)
  (flet ((same-p (entry param)
           (or (string= "*" entry)
               (string= entry param))))
    (when (and (match-hostname pgpass hostname)
               (same-p (pgpass-port pgpass)     port)
               (same-p (pgpass-database pgpass) database)
               (same-p (pgpass-username pgpass) username))
      (pgpass-password pgpass))))

(defun match-pgpass-entries (pgpass-lines hostname port database username)
  "Return matched password from ~/.pgpass or PGPASSFILE, or nil."
  (loop :for pgpass :in pgpass-lines
     :thereis (match-pgpass pgpass hostname port database username)))

(defun match-pgpass-file (hostname port database username)
  "Return matched password from ~/.pgpass or PGPASSFILE, or nil."
  (handler-case
      (let ((pgpass-entries (parse-pgpass-file)))
        (when pgpass-entries
          (match-pgpass-entries pgpass-entries hostname port database username)))
    (condition (e)
      ;; if we had any problem (parsing error in pgpass or otherwise), just
      ;; return a NIL password
      (log-message :warning "Error reading pgass file: ~a" e)
      nil)))

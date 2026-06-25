;;;
;;; Tools to handle MySQL connection and querying
;;;

(in-package :pgloader.source.mysql)

(defvar *connection* nil "Current MySQL connection")

;;;
;;; qmynd maps MySQL charset/collation IDs to Babel encoding keywords, but
;;; several mappings are wrong because MySQL's charset names don't match the
;;; actual encoding stored on disk:
;;;
;;;   * latin1  — MySQL documents it as cp1252 (Windows-1252).  The two share
;;;               all code points in 0x00–0x7F and 0xA0–0xFF but differ in
;;;               0x80–0x9F: ISO 8859-1 treats those 32 positions as C1 control
;;;               characters while cp1252 maps them to printable characters
;;;               (€ 0x80, – 0x96, ™ 0x99, …).  qmynd maps all latin1
;;;               collations to :iso-8859-1 — silently corrupting any byte in
;;;               0x80–0x9F.
;;;
;;;   * latin5  — MySQL uses this name for ISO 8859-9 (Turkish), not ISO 8859-5
;;;               (Cyrillic).  qmynd maps latin5 to :iso-8859-5, which will
;;;               mis-decode the entire 0xA0–0xFF range for Turkish text.
;;;
;;;   * latin7-estonian-cs (collation 20) — qmynd maps it to :iso-8859-7
;;;               (Greek) instead of :iso-8859-13 (Baltic/Latvian), which is
;;;               correct for all other latin7 collations in qmynd.
;;;
;;; cp850   — DOS Latin-1.  The system Babel (cl-babel Debian package) does
;;;           NOT ship an enc-cp850.lisp; returning :cp850 would raise an
;;;           unknown-encoding error at runtime.  qmynd already punts
;;;           cp850-general-ci to :iso-8859-1; we leave that as-is.
;;;           cp850-binary (collation 80) is absent from qmynd's ecase and
;;;           would cause a run-time ecase-failure; we add it here as the same
;;;           :iso-8859-1 punt that qmynd uses for cp850-general-ci.
;;;
;;; We redefine qmynd-impl::mysql-cs-coll-to-character-encoding here (after
;;; qmynd is fully loaded) rather than patching the vendored library, so that
;;; upgrading qmynd cannot silently revert the fix.  Collation IDs are written
;;; as integer literals rather than #.(list ...) reader macros so the
;;; redefinition is safe across SBCL image save/restore.
;;;   latin1-german1-ci=5  latin1-swedish-ci=8   latin1-danish-ci=15
;;;   latin1-german2-ci=31 latin1-binary=47       latin1-general-ci=48
;;;   latin1-general-cs=49 latin1-spanish-ci=94
;;;   latin5-turkish-ci=21 latin5-binary=196
;;;   latin7-estonian-cs=20
;;;   cp850-binary=80
;;;
;; Store the original before redefining so the closure below can delegate.
;; defvar rather than a let-captured variable so the function object is
;; explicitly rooted in the image and survives SBCL save/restore cleanly.
(defvar *qmynd-original-cs-coll-fn*
  #'qmynd-impl::mysql-cs-coll-to-character-encoding)

(defun qmynd-impl::mysql-cs-coll-to-character-encoding (cs-coll)
  ;; Collation IDs as integer literals (no #. reader macros) for safety.
  ;; latin1-german1-ci=5  latin1-swedish-ci=8   latin1-danish-ci=15
  ;; latin1-german2-ci=31 latin1-binary=47       latin1-general-ci=48
  ;; latin1-general-cs=49 latin1-spanish-ci=94
  ;; latin5-turkish-ci=21 latin5-binary=196
  ;; latin7-estonian-cs=20
  ;; cp850-binary=80  (cp850-general-ci=4 left to original: same :iso-8859-1)
  (case cs-coll
    ;; MySQL latin1 is Windows cp1252, not ISO 8859-1.
    ((5 8 15 31 47 48 49 94) :cp1252)
    ;; MySQL latin5 is ISO 8859-9 (Turkish), not ISO 8859-5 (Cyrillic).
    ((21 196) :iso-8859-9)
    ;; latin7-estonian-cs should be ISO 8859-13; qmynd has it as :iso-8859-7.
    ((20) :iso-8859-13)
    ;; cp850-binary (80) absent from qmynd's ecase: same :iso-8859-1 punt.
    ((80) :iso-8859-1)
    (otherwise
     (funcall *qmynd-original-cs-coll-fn* cs-coll))))

;;;
;;; General utility to manage MySQL connection
;;;
(defclass mysql-connection (db-connection)
  ((use-ssl :initarg :use-ssl :accessor myconn-use-ssl)))

(defmethod initialize-instance :after ((myconn mysql-connection) &key)
  "Assign the type slot to mysql."
  (setf (slot-value myconn 'type) "mysql"))

(defmethod clone-connection ((c mysql-connection))
  (let ((clone
         (change-class (call-next-method c) 'mysql-connection)))
    (setf (myconn-use-ssl clone) (myconn-use-ssl c))
    clone))

(defmethod ssl-mode ((myconn mysql-connection))
  "Return non-nil when the connection uses SSL"
  (ecase (myconn-use-ssl myconn)
    (:try  :unspecified)
    (:yes  t)
    (:no   nil)))

(defmethod open-connection ((myconn mysql-connection) &key)
  (setf (conn-handle myconn)
        (if (and (consp (db-host myconn)) (eq :unix (car (db-host myconn))))
            (qmynd:mysql-local-connect :path (cdr (db-host myconn))
                                       :username (db-user myconn)
                                       :password (db-pass myconn)
                                       :database (db-name myconn))
            (qmynd:mysql-connect :host (db-host myconn)
                                 :port (db-port myconn)
                                 :username (db-user myconn)
                                 :password (db-pass myconn)
                                 :database (db-name myconn)
                                 :ssl (ssl-mode myconn))))
  (log-message :debug "CONNECTED TO ~a" myconn)

  ;; apply mysql-settings, if any
  (loop :for (name . value) :in *mysql-settings*
     :for sql := (format nil "set ~a = ~a;" name value)
     :do (query myconn sql))
  ;; return the connection object
  myconn)

(defmethod close-connection ((myconn mysql-connection))
  (qmynd:mysql-disconnect (conn-handle myconn))
  (setf (conn-handle myconn) nil)
  myconn)

(defmethod query ((myconn mysql-connection)
                  sql
                  &key
                    row-fn
                    (as-text t)
                    (result-type 'list))
  "Run SQL query against MySQL connection MYCONN."
  (log-message :sql "MySQL: sending query: ~a" sql)
  (qmynd:mysql-query (conn-handle myconn)
                     sql
                     :row-fn row-fn
                     :as-text as-text
                     :result-type result-type))

;;;
;;; The generic API query is recent, used to look like this:
;;;
(declaim (inline mysql-query))
(defun mysql-query (query &key row-fn (as-text t) (result-type 'list))
  "Execute given QUERY within the current *connection*, and set proper
   defaults for pgloader."
  (query *connection* query
         :row-fn row-fn
         :as-text as-text
         :result-type result-type))


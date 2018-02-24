;;;
;;; Tools to handle MySQL connection and querying
;;;

(in-package :pgloader.source.mysql)

(defvar *connection* nil "Current MySQL connection")

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


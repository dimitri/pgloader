;;;
;;; Tools to query the MS SQL Schema to reproduce in PostgreSQL
;;;

(in-package :pgloader.source.mssql)

(defvar *mssql-db* nil
  "The MS SQL database connection handler.")

;;;
;;; General utility to manage MS SQL connection
;;;
(defclass mssql-connection (db-connection) ())

(defmethod initialize-instance :after ((msconn mssql-connection) &key)
  "Assign the type slot to mssql."
  (setf (slot-value msconn 'type) "mssql"))

(defmethod open-connection ((msconn mssql-connection) &key)
  ;; we can't pass in the port number, set it in the TDSPORT env instead
  (setf (uiop:getenv "TDSPORT") (princ-to-string (db-port msconn)))

  (setf (conn-handle msconn) (mssql:connect (db-name msconn)
                                            (db-user msconn)
                                            (db-pass msconn)
                                            (db-host msconn)))
  ;; apply mssql-settings, if any
  (loop :for (name . value) :in *mssql-settings*
     :for sql := (format nil "set ~a ~a;" name value)
     :do (query msconn sql))

  ;; return the connection object
  msconn)

(defmethod close-connection ((msconn mssql-connection))
  (mssql:disconnect (conn-handle msconn))
  (setf (conn-handle msconn) nil)
  msconn)

(defmethod clone-connection ((c mssql-connection))
  (change-class (call-next-method c) 'mssql-connection))

(defmethod query ((msconn mssql-connection) sql &key)
  "Send SQL query to MSCONN connection."
  (log-message :sql "MSSQL: sending query: ~a" sql)
  (mssql:query sql :connection (conn-handle msconn)))

(defun mssql-query (query)
  "Execute given QUERY within the current *connection*, and set proper
   defaults for pgloader."
  (query *mssql-db* query))

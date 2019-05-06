;;;
;;; SQLite tools connecting to a database
;;;
(in-package :pgloader.source.sqlite)

(defvar *sqlite-db* nil
  "The SQLite database connection handler.")

;;;
;;; Integration with the pgloader Source API
;;;
(defclass sqlite-connection (fd-connection)
  ((has-sequences :initform nil :accessor has-sequences)))

(defmethod initialize-instance :after ((slconn sqlite-connection) &key)
  "Assign the type slot to sqlite."
  (setf (slot-value slconn 'type) "sqlite"))

(defmethod open-connection ((slconn sqlite-connection) &key check-has-sequences)
  (setf (conn-handle slconn)
        (sqlite:connect (fd-path slconn)))
  (log-message :debug "CONNECTED TO ~a" (fd-path slconn))
  (when check-has-sequences
    (let ((sql (format nil (sql "/sqlite/sqlite-sequence.sql"))))
      (log-message :sql "SQLite: ~a" sql)
      (when (sqlite:execute-single (conn-handle slconn) sql)
        (setf (has-sequences slconn) t))))
  slconn)

(defmethod close-connection ((slconn sqlite-connection))
  (sqlite:disconnect (conn-handle slconn))
  (setf (conn-handle slconn) nil)
  slconn)

(defmethod clone-connection ((slconn sqlite-connection))
  (change-class (call-next-method slconn) 'sqlite-connection))

(defmethod query ((slconn sqlite-connection) sql &key)
  (log-message :sql "SQLite: sending query: ~a" sql)
  (sqlite:execute-to-list (conn-handle slconn) sql))


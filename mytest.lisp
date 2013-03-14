;; pg1
;; mysql -h gala3.galaxya.fr -u pg1 -pAFmhKERxD9PVjgQD
;; mysql -h gala4.galaxya.fr -u pg1 -pAFmhKERxD9PVjgQD
;;
;; pg2
;; mysql -h gala3.galaxya.fr -u pg2 -pFhssUAVTDaQZGrLd
;; mysql -h gala4.galaxya.fr -u pg2 -pFhssUAVTDaQZGrLd

(in-package :galaxya-loader)

(defparameter *myconn-host* "gala3.galaxya.fr")
(defparameter *myconn-user* "pg1")
(defparameter *myconn-pass* "AFmhKERxD9PVjgQD")

(setq *myconn-host* "127.0.0.1"
      *myconn-user* "galaxyad"
      *myconn-pass* "gabrielghk")

(defun toto ()
  (cl-mysql:connect :host *myconn-host*
		    :user *myconn-user*
		    :password *myconn-pass*)

  (cl-mysql:query "SET NAMES 'utf8'")
  (cl-mysql:use "galaxya1")
  (prog1
      (cl-mysql:query "SELECT * FROM toto;" :type-map nil)
    (cl-mysql:disconnect)))

(defun mytest ()
  ;; connect
  (cl-mysql:connect :host *myconn-host*
		    :user *myconn-user*
		    :password *myconn-pass*)

  (unwind-protect
       (progn
	 ;; Ensure we're talking utf-8 and connect to DBNAME in MySQL
	 (cl-mysql:query "SET NAMES 'utf8'")
	 (cl-mysql:use "galaxya1")

	 (let* ((sql "SELECT * FROM toto;")
		(q   (cl-mysql:query sql :store nil))
		(rs  (cl-mysql:next-result-set q)))
	   (declare (ignore rs))

	   ;; "SELECT id, email, date_debut, date_redige, ip
	   ;;       FROM commentaires
	   ;;      WHERE date_redige = '0000-00-00' LIMIT 10;"

	   ;; Now fetch MySQL rows
	   (loop
	      for row = (cl-mysql:next-row q :type-map (make-hash-table))
	      while row collect row)))

    ;; free resources
    (cl-mysql:disconnect)))

(defun mytest-to-csv (dbname table-name filename)
  "extract MySQL data to CSV file"
  (cl-mysql:connect :host *myconn-host*
		    :user *myconn-user*
		    :password *myconn-pass*)

  (unwind-protect
       (progn
	 ;; Ensure we're talking utf-8 and connect to DBNAME in MySQL
	 (cl-mysql:query "SET NAMES 'utf8'")
	 (cl-mysql:query "SET character_set_results = utf8;")
	 (cl-mysql:use dbname)

	 (let* ((sql (format nil "SELECT * FROM ~a;" table-name))
		(q   (cl-mysql:query sql :store nil))
		(rs  (cl-mysql:next-result-set q)))
	   (declare (ignore rs))

	   ;; Now fetch MySQL rows directly in the stream
	   (with-open-file (csv filename
				:direction :output
				:if-exists :supersede
				:external-format :utf-8)
	     (loop
		for count from 1
		for row = (cl-mysql:next-row q :type-map (make-hash-table))
		while row
		do (cl-csv:write-csv-row (reformat-row row '(2 3))
					 :stream csv
					 :separator #\;
					 :quote #\"
					 :newline '(#\Newline))
		finally (return count)))))

    ;; free resources
    (cl-mysql:disconnect)))

(defun reformat-row (row date-columns)
  "cl-mysql returns universal date, we want PostgreSQL date strings"
  (loop
     for i from 1
     for col in row
     when (member i date-columns)
     collect (reformat-date col)
     else collect col))

(defun pgtest (mydbname table-name &key (truncate t))
  (cl-mysql:connect :host *myconn-host*
		    :user *myconn-user*
		    :password *myconn-pass*)

  ;; Ensure we're talking utf-8 and connect to DBNAME in MySQL
  (cl-mysql:query "SET NAMES 'utf8'")
  (cl-mysql:query "SET character_set_results = utf8;")
  (cl-mysql:use mydbname)

  ;; TRUNCATE the table in PostgreSQL
  (when truncate
    (format t "TRUNCATE ~a;~%" table-name)
    (pomo:with-connection '("dim" "dim" "none" "localhost")
      (pomo:execute (format nil "truncate ~a;" table-name))))

  ;; Now fetch MySQL rows and feed them to our COPY stream
  (let* ((my-sql (format nil
			 "SELECT * FROM ~a ORDER BY id LIMIT 1;"
			 table-name))
	 (my-q   (cl-mysql:query my-sql :store nil))
	 (my-rs  (cl-mysql:next-result-set my-q))

	 (pgconn '("dim" "dim" "none" "localhost"))
	 (pgstream
	  (cl-postgres:open-db-writer pgconn table-name nil)))

    (declare (ignore my-rs))

    (unwind-protect
	 (loop
	    for count from 0

	    ;; read MySQL data
	    for row = (cl-mysql:next-row my-q :type-map (make-hash-table))
	    while row

	    ;; write it to PostgreSQL
	    do (cl-postgres:db-write-row pgstream
					 (mapcar (lambda (x)
						   (if (null x) :null x))
						 row))

	    finally (return count))
      ;; in case of error, close copier and database
      (cl-postgres:close-db-writer pgstream))))

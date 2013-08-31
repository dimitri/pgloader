;;;; http://xach.com/lisp/db3.lisp
;;
;; db3.lisp
#|

Database file structure

The structure of a dBASE III database file is composed of a header
and data records.  The layout is given below.


dBASE III DATABASE FILE HEADER:

+---------+-------------------+---------------------------------+
|  BYTE   |     CONTENTS      |          MEANING                |
+---------+-------------------+---------------------------------+
|  0      |  1 byte           | dBASE III version number        |
|         |                   |  (03H without a .DBT file)      |
|         |                   |  (83H with a .DBT file)         |
+---------+-------------------+---------------------------------+
|  1-3    |  3 bytes          | date of last update             |
|         |                   |  (YY MM DD) in binary format    |
+---------+-------------------+---------------------------------+
|  4-7    |  32 bit number    | number of records in data file  |
+---------+-------------------+---------------------------------+
|  8-9    |  16 bit number    | length of header structure      |
+---------+-------------------+---------------------------------+
|  10-11  |  16 bit number    | length of the record            |
+---------+-------------------+---------------------------------+
|  12-31  |  20 bytes         | reserved bytes (version 1.00)   |
+---------+-------------------+---------------------------------+
|  32-n   |  32 bytes each    | field descriptor array          |
|         |                   |  (see below)                    | --+
+---------+-------------------+---------------------------------+   |
|  n+1    |  1 byte           | 0DH as the field terminator     |   |
+---------+-------------------+---------------------------------+   |
                                                                    |
                                                                    |
A FIELD DESCRIPTOR:      <------------------------------------------+

+---------+-------------------+---------------------------------+
|  BYTE   |     CONTENTS      |          MEANING                |
+---------+-------------------+---------------------------------+
|  0-10   |  11 bytes         | field name in ASCII zero-filled |
+---------+-------------------+---------------------------------+
|  11     |  1 byte           | field type in ASCII             |
|         |                   |  (C N L D or M)                 |
+---------+-------------------+---------------------------------+
|  12-15  |  32 bit number    | field data address              |
|         |                   |  (address is set in memory)     |
+---------+-------------------+---------------------------------+
|  16     |  1 byte           | field length in binary          |
+---------+-------------------+---------------------------------+
|  17     |  1 byte           | field decimal count in binary   |
+---------+-------------------+---------------------------------+
|  18-31  |  14 bytes         | reserved bytes (version 1.00)   |
+---------+-------------------+---------------------------------+


The data records are layed out as follows:

     1. Data records are preceeded by one byte that is a space (20H) if the
        record is not deleted and an asterisk (2AH) if it is deleted.

     2. Data fields are packed into records with  no  field separators or
        record terminators.

     3. Data types are stored in ASCII format as follows:

     DATA TYPE      DATA RECORD STORAGE
     ---------      --------------------------------------------
     Character      (ASCII characters)
     Numeric        - . 0 1 2 3 4 5 6 7 8 9
     Logical        ? Y y N n T t F f  (? when not initialized)
     Memo           (10 digits representing a .DBT block number)
     Date           (8 digits in YYYYMMDD format, such as
                    19840704 for July 4, 1984)

|#
(defpackage :db3
  (:shadow #:type)
  (:use :cl))

(in-package :db3)


;;; reading binary stuff
(defun read-uint32 (stream)
  (loop repeat 4
        for offset from 0 by 8
        for value = (read-byte stream)
          then (logior (ash (read-byte stream) offset) value)
        finally (return value)))

(defun read-uint16 (stream)
  (loop repeat 2
        for offset from 0 by 8
        for value = (read-byte stream)
          then (logior (ash (read-byte stream) offset) value)
        finally (return value)))



;;; objects

(defclass db3 ()
  ((version-number :accessor version-number)
   (last-update :accessor last-update)
   (record-count :accessor record-count)
   (header-length :accessor header-length)
   (record-length :accessor record-length)
   (fields :accessor fields)))


(defclass db3-field ()
  ((name :accessor name)
   (type :accessor type)
   (data-address :accessor data-address)
   (field-length :accessor field-length)
   (field-count :accessor field-count)))


(defun asciiz->string (array)
  (let* ((string-length (or (position 0 array)
                            (length array)))
         (string (make-string string-length)))
    (loop for i below string-length
          do (setf (schar string i) (code-char (aref array i))))
    string))

(defun ascii->string (array)
  (map 'string #'code-char array))
    

(defun load-field-descriptor (stream)
  (let ((field (make-instance 'db3-field))
        (name (make-array 11 :element-type '(unsigned-byte 8))))
    (read-sequence name stream)
    (setf (name field) (asciiz->string name)
          (type field) (code-char (read-byte stream))
          (data-address field) (read-uint32 stream)
          (field-length field) (read-byte stream)
          (field-count field) (read-byte stream))
    (loop repeat 14 do (read-byte stream))
    field))


(defmethod field-count ((db3 db3))
  (1- (/ (1- (header-length db3)) 32)))


(defmethod load-header ((db3 db3) stream)
  (let ((version (read-byte stream)))
    (unless (= version #x03)
      (error "Can't handle this file"))
    (let ((year (read-byte stream))
          (month (read-byte stream))
          (day (read-byte stream)))
      (setf (version-number db3) version 
            (last-update db3) (list year month day)
            (record-count db3) (read-uint32 stream)
            (header-length db3) (read-uint16 stream)
            (record-length db3) (read-uint16 stream))
      (file-position stream 32)
      (setf (fields db3) (loop repeat (field-count db3)
                               collect (load-field-descriptor stream)))
      (assert (= (read-byte stream) #x0D))
      db3)))


(defmethod convert-field (type data)
  (ascii->string data))

(defmethod convert-field ((type (eql #\C)) data)
  (ascii->string data))
  

(defmethod load-field (type length stream)
  (let ((field (make-array length)))
    (read-sequence field stream)
    (convert-field type field)))

(defmethod load-record ((db3 db3) stream)
  (read-byte stream)
  (loop with record = (make-array (field-count db3))
        for i below (field-count db3)
        for field in (fields db3)
        do (setf (svref record i)
                 (load-field (type field) (field-length field) stream))
        finally (return record)))


(defun write-record (record stream)
  (loop for field across record
        do
        (write-char #\" stream)
        (write-string field stream)
        (write-string "\"," stream))
  (terpri stream))


(defun dump-db3 (input output)
  (with-open-file (stream input :direction :input
                          :element-type '(unsigned-byte 8))
    (with-open-file (ostream output :direction :output
                             :element-type 'character)
      (let ((db3 (make-instance 'db3)))
        (load-header db3 stream)
        (loop repeat (record-count db3)
              do (write-record (load-record db3 stream) ostream))
        db3))))

(defun sample-db3 (input ostream &key (sample-size 10))
  (with-open-file (stream input :direction :input
                          :element-type '(unsigned-byte 8))
    (let ((db3 (make-instance 'db3)))
      (load-header db3 stream)
      (loop
	 :repeat sample-size
	 :do (format ostream "~s~%" (load-record db3 stream) ostream))
      db3)))

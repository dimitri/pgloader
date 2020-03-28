;;;
;;; Monkey patch mssql to add missing bits, will cook a patch later.
;;;

(in-package :mssql)

;;
;; See freetds/include/freetds/proto.h for reference
;;
(defcenum %syb-value-type
  (:syb-char  47)
  (:syb-varchar  39)
  (:syb-intn  38)
  (:syb-int1  48)
  (:syb-int2  52)
  (:syb-int4  56)
  (:syb-int8  127)
  (:syb-flt8  62)
  (:syb-datetime  61)
  (:syb-bit  50)
  (:syb-text  35)
  (:syb-image  34)
  (:syb-money4  122)
  (:syb-money  60)
  (:syb-datetime4  58)
  (:syb-real  59)
  (:syb-binary  45)
  (:syb-varbinary  37)
  (:syb-bitn 104)
  (:syb-numeric  108)
  (:syb-decimal  106)
  (:syb-fltn  109)
  (:syb-moneyn  110)
  (:syb-datetimn  111)

  ;; MS only types
  (:syb-nvarchar 103)
  ;(:syb-int8 127)
  (:xsy-bchar 175)
  (:xsy-bvarchar 167)
  (:xsy-bnvarchar 231)
  (:xsy-bnchar 239)
  (:xsy-bvarbinary 165)
  (:xsy-bbinary 173)
  (:syb-unique 36)
  (:syb-variant 98)
  (:syb-msudt 240)
  (:syb-msxml 241)
  (:syb-msdate 40)
  (:syb-mstime 41)
  (:syb-msdatetime2 42)
  (:syb-msdatetimeoffset 43)

  ;; Sybase only types
  (:syb-longbinary 225)
  (:syb-uint1 64)
  (:syb-uint2 65)
  (:syb-uint4 66)
  (:syb-uint8 67)
  (:syb-blob 36)
  (:syb-boundary 104)
  (:syb-date 49)
  (:syb-daten 123)
  (:syb-5int8 191)
  (:syb-interval 46)
  (:syb-longchar 175)
  (:syb-sensitivity 103)
  (:syb-sint1 176)
  (:syb-time 51)
  (:syb-timen 147)
  (:syb-uintn 68)
  (:syb-unitext 174)
  (:syb-xml 163)
  )

(defun unsigned-to-signed (byte n)
  (declare (type fixnum n) (type unsigned-byte byte))
  (logior byte (- (mask-field (byte 1 (1- (* n 8))) byte))))

(defun sysdb-data-to-lisp (%dbproc data type len)
  (let ((syb-type (foreign-enum-keyword '%syb-value-type type)))
    (case syb-type
      ;; we accept emtpy string (len is 0)
      ((:syb-char :syb-varchar :syb-text :syb-msxml)
       (foreign-string-to-lisp data :count len))

      (otherwise
       ;; other types must have a non-zero len now, or we just return nil.
       (if (> len 0)
           (case syb-type
             ((:syb-bit :syb-bitn) (mem-ref data :int))
             (:syb-int1 (unsigned-to-signed (mem-ref data :unsigned-int) 1))
             (:syb-int2 (unsigned-to-signed (mem-ref data :unsigned-int) 2))
             (:syb-int4 (unsigned-to-signed (mem-ref data :unsigned-int) 4))
             (:syb-int8 (mem-ref data :int8))
             (:syb-real (mem-ref data :float))
             (:syb-flt8 (mem-ref data :double))
             ((:syb-datetime
               :syb-datetime4
               :syb-msdate
               :syb-mstime
               :syb-msdatetime2
               :syb-msdatetimeoffset)
              (with-foreign-pointer (%buf +numeric-buf-sz+)
                (let ((count
                       (%dbconvert %dbproc
                                   type
                                   data
                                   -1
                                   :syb-char
                                   %buf
                                   +numeric-buf-sz+)))
                 (foreign-string-to-lisp %buf :count count))))
             ((:syb-money :syb-money4 :syb-decimal :syb-numeric)
              (with-foreign-pointer (%buf +numeric-buf-sz+)
                (let ((count
                       (%dbconvert %dbproc
                                   type
                                   data
                                   -1
                                   :syb-char
                                   %buf
                                   +numeric-buf-sz+)))
                 (parse-number:parse-number
                  (foreign-string-to-lisp %buf :count count )))))
             ((:syb-image :syb-binary :syb-varbinary :syb-blob)
              (let ((vector (make-array len :element-type '(unsigned-byte 8))))
                (dotimes (i len)
                  (setf (aref vector i) (mem-ref data :uchar i)))
                vector))
             (otherwise (error "not supported type ~A"
                               (foreign-enum-keyword '%syb-value-type type)))))))))

;; (defconstant +dbbuffer+ 14)

;; (define-sybdb-function ("dbsetopt" %dbsetopt) %RETCODE
;;   (dbproc %DBPROCESS)
;;   (option :int)
;;   (char-param :pointer)
;;   (int-param :int))

(defun map-query-results (query &key row-fn (connection *database*))
  "Map the query results through the map-fn function."
  (let ((%dbproc (slot-value connection 'dbproc))
        (cffi:*default-foreign-encoding* (slot-value connection 'external-format)))
    (with-foreign-string (%query query)
      (%dbcmd %dbproc %query))
    (%dbsqlexec %dbproc)
    (unwind-protect
         (unless (= +no-more-results+ (%dbresults %dbproc))
           (loop :for rtc := (%dbnextrow %dbproc)
              :until (= rtc +no-more-rows+)
              :do (let ((row (make-array (%dbnumcols %dbproc))))
                    (loop :for i :from 1 :to (%dbnumcols %dbproc)
                       :for value
                       := (restart-case
                              (sysdb-data-to-lisp %dbproc
                                                  (%dbdata %dbproc i)
                                                  (%dbcoltype %dbproc i)
                                                  (%dbdatlen %dbproc i))
                            (use-nil ()
                              :report "skip this column's value and use nil instead."
                              nil)
                            (use-empty-string ()
                              :report "skip this column's value and use empty-string instead."
                              "")
                            (use-value (value) value))
                       :do (setf (aref row (- i 1)) value))

                    (funcall row-fn row))))
      (%dbcancel %dbproc))))

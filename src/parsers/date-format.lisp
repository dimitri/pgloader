;;;
;;; Parse PostgreSQL to_char() date format strings and transform them into a
;;; lisp date format that the parse-date-string function knows how to deal
;;; with.
;;;
;;; http://www.postgresql.org/docs/current/static/functions-formatting.html#FUNCTIONS-FORMATTING-DATETIME-TABLE
;;;

(in-package #:pgloader.parse-date)

(defvar *century* 2000)

(defun parse-date-string
    (date-string
     &optional (format '((:year     0  4)
			 (:month    4  6)
			 (:day      6  8)
			 (:hour     8 10)
			 (:minute  10 12)
			 (:seconds 12 14)
                         (:msecs   nil nil)
                         (:usecs   nil nil)

                         (:am nil nil)
                         (:pm nil nil))))
  "Apply this function when input date in like '20041002152952'"
  ;; only process non-zero dates
  (declare (type (or null string) date-string))
  (cond ((null date-string)                nil)
        ((string= date-string "")          nil)
        (t
         (destructuring-bind (&key year month day hour minute seconds
                                   am pm
                                   msecs usecs
                                   &allow-other-keys)
             (loop
                :for (name start end) :in format
                :for ragged-end := (when end
                                     (cond ((member name '(:msecs :usecs))
                                            ;; take any number of digits up to
                                            ;; the specified field length
                                            ;; (less digits are allowed)
                                            (when (<= start (length date-string))
                                              (min end (length date-string))))
                                           (t end)))
                :when (and start ragged-end)
                :append (list name (subseq date-string start ragged-end)))
           (if (or (string= year  "0000")
                   (string= month "00")
                   (string= day   "00"))
               nil
               (with-output-to-string (s)
                 ;; when given a full date format, format the date part
                 (when (and (assoc :year format)
                            (assoc :month format)
                            (assoc :day format))
                   (format s "~a-~a-~a "
                           (let ((yint (parse-integer year)))
                             ;; process 2-digits year formats specially
                             (if (<= (length year) 2) (+ *century* yint) yint))
                           month
                           day))

                 ;; now format the time part
                 (format s "~a:~a:~a"
                         (if hour
                             (let ((hint (parse-integer hour)))
                               (cond ((and am (= hint 12)) "00")
                                     ((and pm (= hint 12)) "12")
                                     ((and pm (< hint 12)) (+ hint 12))
                                     (t hour)))
                             "00")
                         (or minute "00")
                         (or seconds "00"))

                 ;; and maybe format the micro seconds part
                 (if usecs (format s ".~a" usecs)
                     (when msecs (format s ".~a" msecs)))))))))


;;;
;;; Parse the date format
;;;   YYYY-MM-DD HH24-MI-SS
;;;
(defvar *offset* 0)

(defun parse-date-format (format-string)
  "Parse a given format string and return a format specification for
   parse-date-string"
  (let ((*offset* 0))
    (remove nil (parse 'format-string format-string))))

(defrule format-string (* (or year
                              month
                              day
                              hour
                              ampm
                              minute
                              seconds
                              milliseconds
                              microseconds
                              noise)))

(defrule noise (+ (or #\: #\- #\. #\Space #\* #\# #\@ #\/ #\\ "T"))
  (:lambda (x) (incf *offset* (length (text x))) nil))

(defrule year (or year4 year3 year2))
(defrule year4  "YYYY"  (:lambda (x) (build-format-spec x :year 4)))
(defrule year3  "YYY"   (:lambda (x) (build-format-spec x :year 3)))
(defrule year2  "YY"    (:lambda (x) (build-format-spec x :year 2)))

(defrule month mm)
(defrule mm "MM"   (:lambda (x) (build-format-spec x :month 2)))

(defrule day dd)
(defrule dd "DD"   (:lambda (x) (build-format-spec x :day 2)))

(defrule hour (or hh24 hh12 hh))
(defrule hh   "HH"   (:lambda (x) (build-format-spec x :hour 2)))
(defrule hh12 "HH12" (:lambda (x) (build-format-spec x :hour 2)))
(defrule hh24 "HH24" (:lambda (x) (build-format-spec x :hour 2)))

(defrule ampm (or am pm am-dot pm-dot))
(defrule am     (or "AM" "am" )    (:lambda (x) (build-format-spec x :am 2)))
(defrule am-dot (or "a.m." "A.M.") (:lambda (x) (build-format-spec x :am 4)))
(defrule pm     (or "PM" "pm")     (:lambda (x) (build-format-spec x :pm 2)))
(defrule pm-dot (or "p.m." "P.M.") (:lambda (x) (build-format-spec x :pm 4)))

(defrule minute mi)
(defrule mi "MI" (:lambda (x) (build-format-spec x :minute 2)))

(defrule seconds ss)
(defrule ss "SS" (:lambda (x) (build-format-spec x :seconds 2)))

(defrule milliseconds ms)
(defrule ms "MS" (:lambda (x) (build-format-spec x :msecs 4)))

(defrule microseconds us)
(defrule us "US" (:lambda (x) (build-format-spec x :usecs 6)))

(defun build-format-spec (format-string part len)
  (declare (ignore format-string))
  (prog1
      (list part *offset* (+ *offset* len))
    (incf *offset* len)))

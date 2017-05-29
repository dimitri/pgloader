;;;
;;; Tools to get the list of query from the model.sql, api.sql and sql/*.sql
;;; files, which remains usable as-is interactively (hence the injecting
;;; trick)
;;;
(in-package #:pgloader.sql)

(defstruct parser
  filename
  (stream  (make-string-output-stream))
  (state   :eat)
  tags)

(defmethod print-object ((p parser) stream)
  (print-unreadable-object (p stream :type t :identity t)
    (with-slots (state tags) p
      (format stream "~a {~{~s~^ ~}}" state tags))))

(defmethod push-new-tag ((p parser))
  "Add a new element on the TAGS slot, a stack"
  (let ((tag (make-array 42
                         :fill-pointer 0
                         :adjustable t
                         :element-type 'character)))
    (push tag (parser-tags p))))

(defmethod extend-current-tag ((p parser) char)
  "The TAGS slot of the parser is a stack, maintain it properly."
  (declare (type character char))
  (assert (not (null (parser-tags p))))
  (vector-push-extend char (first (parser-tags p))))

(defmethod format-current-tag ((p parser) &optional (stream (parser-stream p)))
  "Output the current tag to the current stream."
  (format stream "$~a$" (coerce (first (parser-tags p)) 'string)))

(defmethod maybe-close-tags ((p parser) &optional (stream (parser-stream p)))
  "If the two top tags in the TAGS slot of the parser P are the
   same (compared using EQUALP), then pop them out of the stack and print
   the closing tag to STREAM."
  (when (and (< 1 (length (parser-tags p)))
             (equalp (first (parser-tags p))
                     (second (parser-tags p))))
    ;; format the tag in the stream and POP both entries
    (format-current-tag p stream)
    (pop (parser-tags p))
    (pop (parser-tags p))
    ;; and return t
    t))

(defmethod pop-current-tag ((p parser))
  "Remove current tag entry"
  (pop (parser-tags p)))

(defmethod reset-state ((p parser))
  "Depending on the current tags stack, set P state to either :eat or :eqt"
  (setf (parser-state p) (if (null (parser-tags p)) :eat :eqt)))

#|
Here's a test case straigth from the PostgreSQL docs:

(with-input-from-string (s "
create function f(text)
   returns bool
   language sql
as $function$
BEGIN
    RETURN ($1 ~ $q$[\\t\\r\\n\\v\\\\]$q$);
END;
$function$;")
        (parse-query s (make-parser)))


Another test case for the classic quotes:

      (with-pgsql-connection ("pgsql:///pginstall")
        (query
         (with-input-from-string (s "select E'\\';' as \";\";")
           (parse-query s)) :alists))

      should return
      (((:|;| . "';")))
|#

(defun parse-query (stream &optional (state (make-parser)))
  "Read a SQL query from STREAM, starting at whatever the current position is.

   Returns another SQL query each time it's called, or NIL when EOF is
   reached expectedly. Signal end-of-file condition when reaching EOF in the
   middle of a query.

   See the following docs for some of the parser complexity background:

    http://www.postgresql.org/docs/9.3/static/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING

   Parser states are:

     - EAT    reading the query
     - TAG    reading a tag that could be an embedded $x$ tag or a closing tag
     - EOT    End Of Tag
     - EQT    Eat Quoted Text
     - EDQ    Eat Double-Quoted Text (identifiers)
     - EOQ    done reading the query
     - ESC    read espaced text (with backslash)"
  (handler-case
      (loop
         :until (eq :eoq (parser-state state))
         :for char := (read-char stream)
         :do (case char
               (#\\       (case (parser-state state)
                            (:esc    (setf (parser-state state) :eqt))
                            (:eqt    (setf (parser-state state) :esc)))

                          (write-char char (parser-stream state)))

               (#\'       (case (parser-state state)
                            (:eat    (setf (parser-state state) :eqt))
                            (:esc    (setf (parser-state state) :eqt))
                            (:eqt    (setf (parser-state state) :eat))
                            (:tag
                             (progn
                               ;; a tag name can't contain a single-quote
                               ;; get back to previous state
                               (let ((tag (pop-current-tag state)))
                                 (format (parser-stream state) "$~a" tag))
                               (reset-state state))))

                          (write-char char (parser-stream state)))

               (#\"       (case (parser-state state)
                            (:eat    (setf (parser-state state) :edq))
                            (:edq    (setf (parser-state state) :eat)))

                          (write-char char (parser-stream state)))

               (#\$       (case (parser-state state)
                            (:eat    (setf (parser-state state) :tag))
                            (:eqt    (setf (parser-state state) :tag))
                            (:tag    (setf (parser-state state) :eot)))

                          ;; we act depending on the NEW state
                          (case (parser-state state)
                            (:eat (write-char char (parser-stream state)))
                            (:edq (write-char char (parser-stream state)))

                            (:tag (push-new-tag state))

                            (:eot       ; check the tag stack
                             (cond ((= 1 (length (parser-tags state)))
                                    ;; it's an opening tag, collect the text now
                                    (format-current-tag state)
                                    (reset-state state))

                                   (t   ; are we closing the current tag?
                                    (if (maybe-close-tags state)
                                        (reset-state state)

                                        ;; not the same tags, switch state back
                                        ;; don't forget to add the opening tag
                                        (progn
                                          (format-current-tag state)
                                          (setf (parser-state state) :eqt))))))))

               (#\;       (case (parser-state state)
                            (:eat      (setf (parser-state state) :eoq))
                            (otherwise (write-char char (parser-stream state)))))

               (otherwise (cond ((member (parser-state state) '(:eat :eqt :edq))
                                 (write-char char (parser-stream state)))

                                ;; see
                                ;; http://www.postgresql.org/docs/9.4/static/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-ESCAPE
                                ;; we re-inject whatever we read in the \x
                                ;; syntax into the stream and let PostgreSQL
                                ;; be the judge of what it means.
                                ((member (parser-state state) '(:esc))
                                 (write-char char (parser-stream state))
                                 (setf (parser-state state) :eqt))

                                ((member (parser-state state) '(:tag))
                                 ;; only letters are allowed in tags
                                 (if (alpha-char-p char)
                                     (extend-current-tag state char)

                                     (progn
                                       ;; not a tag actually: remove the
                                       ;; parser-tags entry and push back its
                                       ;; contents to the main output stream
                                       (let ((tag (pop-current-tag state)))
                                         (format (parser-stream state)
                                                 "$~a~c"
                                                 tag
                                                 char))
                                       (reset-state state)))))))
         :finally (return
                    (get-output-stream-string (parser-stream state))))
    (end-of-file (e)
      (unless (eq :eat (parser-state state))
        (error e)))))

(defun read-lines (filename &optional (q (make-string-output-stream)))
  "Read lines from given filename and return them in a stream. Recursively
   apply \i include instructions."
  (with-open-file (s filename :direction :input)
    (loop
       for line = (read-line s nil)
       while line
       do (if (or (and (> (length line) 3)
                       (string= "\\i " (subseq line 0 3)))
                  (and (> (length line) 4)
                       (string= "\\ir " (subseq line 0 4))))
	      (let ((include-filename
		     (merge-pathnames (subseq line 3)
				      (directory-namestring filename))))
	       (read-lines include-filename q))
	      (format q "~a~%" line))
       finally (return q))))

(defun read-queries (filename)
  "read SQL queries in given file and split them, returns a list"
  (let ((file-content (get-output-stream-string (read-lines filename))))
    (with-input-from-string (s file-content)
      (loop :for query := (parse-query s)
         :while query
         :collect query))))



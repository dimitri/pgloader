;;;
;;; Tools to translate MS SQL index definitions into PostgreSQL
;;;    CREATE INDEX ... WHERE ...
;;; clauses.
;;;

(in-package #:pgloader.mssql.index-filter)

(defmethod translate-index-filter ((table table)
                                   (index pgsql-index)
                                   (sql-dialect (eql 'copy-mssql)))
  "Transform given MS SQL index filter to PostgreSQL slang."
  (labels ((process-expr (expression)
             (destructuring-bind (operator identifier &optional argument)
                 expression
               (let* ((pg-id (apply-identifier-case identifier))
                      (col   (find pg-id
                                   (table-column-list table)
                                   :key #'column-name
                                   :test #'string=)))
                 (assert (not (null col)))
                 (list operator pg-id (when argument
                                        (funcall (column-transform col)
                                                 argument)))))))
    (when (pgsql-index-filter index)
      (let* ((raw-expr (parse-index-filter-clause (pgsql-index-filter index)))
             (pg-expr
              (loop :for node :in raw-expr
                 :collect (typecase node
                            (string node) ; that's "and" / "or"
                            (list   (process-expr node))))))
        ;; now reformat the expression into SQL
        (with-output-to-string (s)
          (loop :for node :in pg-expr
             :do (typecase node
                   (string (format s "  ~a  " node))
                   (list
                    (destructuring-bind (op id &optional arg) node
                      (format s "~a ~a~@[ '~a'~]" id op arg))))))))))

(defun parse-index-filter-clause (filter)
  "Parse the filter clause for a MS SQL index, see
    https://technet.microsoft.com/en-us/library/cc280372.aspx"
  (parse 'mssql-index-where-clause filter))


;;;
;;; Esrap parser for the index WHERE clause
;;;
;;;  Examples:
;;;     "([deleted]=(0))"
;;;     EndDate IS NOT NULL
;;;     EndDate IN ('20000825', '20000908', '20000918')
;;;     EndDate IS NOT NULL AND ComponentID = 5 AND StartDate > '01/01/2008'
;;;
(defrule whitespace (+ (or #\Space #\Tab #\Newline)) (:constant #\Space))

(defrule punct (or #\, #\- #\_) (:text t))

(defrule namestring (and (or #\_ (alpha-char-p character))
			 (* (or (alpha-char-p character)
				(digit-char-p character)
				punct)))
  (:text t))

(defrule mssql-bracketed-identifier (and "[" namestring "]")
  (:lambda (id) (text (second id))))

(defrule mssql-identifier (or mssql-bracketed-identifier namestring))

(defrule =  "="  (:constant :=))
(defrule <= "<=" (:constant :<=))
(defrule >= ">=" (:constant :>=))
(defrule <  "<"  (:constant :<))
(defrule >  ">"  (:constant :>))
(defrule <> "<>" (:constant :<>))
(defrule != "!=" (:constant :<>))

(defrule mssql-operator (and (? whitespace) (or = <= < >= > <> !=))
  (:lambda (op) (second op)))

(defrule number (+ (digit-char-p character)) (:text t))
(defrule quoted (and "'" (+ (not "'")) "'")  (:lambda (q) (text (second q))))

(defrule mssql-constant-parens (and (? whitespace)
                                    "("
                                    (or number quoted)
                                    ")")
  (:function third))

(defrule mssql-constant-no-parens (and (? whitespace) (or number quoted))
  (:function second))

(defrule mssql-constant (or mssql-constant-parens mssql-constant-no-parens))

(defrule mssql-is-not-null (and (? whitespace)
                                (~ "is") whitespace
                                (~ "not") whitespace
                                (~ "null"))
  (:constant :is-not-null))

(defrule mssql-is-null (and (? whitespace)
                            (~ "is") whitespace
                            (~ "null"))
  (:constant :is-null))

(defrule mssql-where-is-null (and mssql-identifier (or mssql-is-null
                                                       mssql-is-not-null))
  (:lambda (is-null) (list (second is-null) (first is-null))))

(defrule mssql-where-id-op-const
    (and mssql-identifier mssql-operator mssql-constant)
  (:lambda (op) (list (second op) (first op) (third op))))

(defrule mssql-where-const-op-id
    (and mssql-constant mssql-operator mssql-identifier)
  ;; always put identifier first
  (:lambda (op) (list (second op) (third op) (first op))))

(defrule mssql-where-op (or mssql-where-id-op-const
                            mssql-where-const-op-id))

(defrule another-constant (and "," (? whitespace) mssql-constant)
  (:function third))

(defrule mssql-in-list (and "(" mssql-constant (* another-constant) ")")
  (:lambda (in) (list* (second in) (third in))))

(defrule mssql-where-in (and (? whitespace) (~ "in") mssql-in-list)
  (:function third))

(defrule mssql-where-between (and (? whitespace)
                                  mssql-identifier
                                  (~ "between")
                                  mssql-constant
                                  (~ "and")
                                  mssql-constant)
  (:function rest))

(defrule mssql-index-filter (and (? "(")
                                 (? whitespace)
                                 (or mssql-where-op
                                     mssql-where-is-null
                                     mssql-where-in
                                     mssql-where-between)
                                 (? whitespace)
                                 (? ")"))
  (:function third))

(defrule another-index-filter (and (? whitespace)
                                   (or (~ "and") (~ "or"))
                                   (? whitespace)
                                   mssql-index-filter)
  (:lambda (another) (list (second another) (fourth another))))

(defrule mssql-index-where-clause (and mssql-index-filter
                                       (* another-index-filter))
  (:lambda (where)
    (list* (first where)
           (loop :for (logical-op filter) :in (second where)
              :collect logical-op
              :collect filter))))

;;;
;;; Script used to prepare a pgloader bundle
;;;

;; fetch a list of recent  candidates with
;; (subseq (ql-dist:available-versions (ql-dist:dist "quicklisp")) 0 5)
;;
;; the 2017-06-30 QL release is broken, avoid it.
;;
(defvar *ql-dist* :latest)

(defvar *ql-dist-url-format*
  "http://beta.quicklisp.org/dist/quicklisp/~a/distinfo.txt")

(let ((dist (if (eq :latest *ql-dist*)
                (cdr
                 ;; available-versions is an alist of (date . url)
                 (first
                  (ql-dist:available-versions (ql-dist:dist "quicklisp"))))
                (format nil *ql-dist-url-format* *ql-dist*))))
  (ql-dist:install-dist dist :prompt nil :replace t)
  (ql:bundle-systems '("pgloader" "buildapp") :to *bundle-dir*))
(quit)

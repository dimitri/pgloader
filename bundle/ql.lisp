;;;
;;; Script used to prepare a pgloader bundle
;;;

;; fetch a list of recent  candidates with
;; (subseq (ql-dist:available-versions (ql-dist:dist "quicklisp")) 0 5)
;;
;; the 2017-06-30 QL release is broken, avoid it.
;;
(defvar *ql-dist*
  "http://beta.quicklisp.org/dist/quicklisp/2017-04-03/distinfo.txt")

(ql-dist:install-dist *ql-dist* :prompt nil :replace t)
(ql:bundle-systems '("pgloader" "buildapp") :to *bundle-dir*)
(quit)

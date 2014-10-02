;;;
;;; Monkey patch metaband-bind macro to ignore nil:
;;;
;;;  https://github.com/gwkkwg/metabang-bind/issues/9
;;;

(in-package #:metabang.bind)

(defun var-ignorable-p (var)
  (and (symbolp var) (string= (symbol-name var) (symbol-name '_))))

;;;
;;; Allow the pgloader load command to be a Mustache Template.
;;;
;;; Variables are to be found either in the OS environment for the process,
;;; or in the .ini file given as a --context command line argument.
;;;

(in-package #:pgloader.parser)

(defun apply-template (string)
  (mustache:render* string *context*))

(defun initialize-context (filename)
  "Initialize a context from the environment variables and from the given
   context-filename (might be nil). CONTEXT-FILENAME is an INI file."

  (when filename
    (setf *context* (read-ini-file filename))))

(defun read-ini-file (filename)
  (let ((ini (ini:make-config)))
    (ini:read-files ini (list filename))

    (loop :for section :in (ini:sections ini)
       :append (loop :for option :in (ini:options ini section)
                  :for key := (string-upcase option)
                  :for val := (ini:get-option ini section option)
                  :collect (cons key val)))))


;;;
;;; cl-mustache doesn't read variables from the environment, and we want to.
;;; cl-mustache uses CLOS for finding values in a context from a key, so we
;;; can derive that.
;;;
(defmethod mustache::context-get :around ((key string) (context hash-table))
  (multiple-value-bind (data find)
      (call-next-method)
    (if find
        (values data find)
        (context-get-from-environment key))))

(defmethod mustache::context-get :around ((key string) (context null))
  (multiple-value-bind (data find)
      (call-next-method)
    (if find
        (values data find)
        (context-get-from-environment key))))

(defun context-get-from-environment (key)
  (let ((val (uiop:getenv key)))
    (if val
        (values val t)
        (values))))

;;; facility to easily run the program

(ql:quickload :pgloader)
(in-package :pgloader)

(setq *myconn-host* "localhost"
      *myconn-user* "debian-sys-maint"
      *myconn-pass* "vtmMI04yBZlFprYm")

(loop
   for test in (list #'pgloader.mysql:stream-database
		     #'pgloader.mysql:export-database
		     #'pgloader.mysql:export-import-database)
   do
     (format t "~&~%Testing ~a:~%~%" test)
     (funcall test "yagoa" :only-tables '("membres" "sav_themes")))



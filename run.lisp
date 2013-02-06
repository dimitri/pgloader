;;; facility to easily run the program

(ql:quickload :pgloader)
(in-package :pgloader)

(setq *myconn-host* "localhost"
      *myconn-user* "debian-sys-maint"
      *myconn-pass* "vtmMI04yBZlFprYm")

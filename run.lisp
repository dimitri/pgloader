;;; facility to easily run the program

(ql:quickload :galaxya-loader)

(in-package :galaxya-loader)

(let ((*pgcon*         '("galaxya" "none" "localhost" :port 5432))
      ((csv-path-root* "/home/cyb/csv")))

  ;; when we're ready we do that
  ;; (load-all-databases))

  ;; meanwhile
  (load-database-tables "weetix"))

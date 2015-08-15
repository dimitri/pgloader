;;;
;;; Generic API for pgloader sources
;;;
(in-package :pgloader.sources)

(defmacro push-to-end (item place)
  `(setf ,place (nconc ,place (list ,item))))

;;;
;;; TODO: stop using anonymous data structures for database catalogs,
;;; currently list of alists of lists... the madness has found its way in
;;; lots of places tho.
;;;

;;;
;;; A database catalog is a list of schema each containing a list of tables,
;;; each being a list of columns.
;;;
;;; Column structures details depend on the specific source type and are
;;; implemented in each source separately.
;;;
(defstruct schema name tables)
(defstruct table schema name qualified-name columns)



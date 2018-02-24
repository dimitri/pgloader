;;;
;;; Generic API for pgloader data loading and database migrations.
;;;
(in-package :pgloader.load)

(defgeneric copy-from (source &key)
  (:documentation
   "Load data from SOURCE into its target as defined by the SOURCE object."))

;; That one is more an export than a load. It always export to a single very
;; well defined format, the importing utility is defined in
;; src/pgsql-copy-format.lisp

(defgeneric copy-to (source filename)
  (:documentation
   "Load data from SOURCE and serialize it into FILENAME, using PostgreSQL
    COPY TEXT format."))

;; The next generic function is only to get instanciated for sources
;; actually containing more than a single source item (tables, collections,
;; etc)

(defgeneric copy-database (source
			   &key
                             worker-count
                             concurrency
                             max-parallel-create-index
			     truncate
			     data-only
			     schema-only
			     create-tables
			     include-drop
                             foreign-keys
			     create-indexes
			     reset-sequences
                             disable-triggers
                             materialize-views
                             set-table-oids
                             including
                             excluding)
  (:documentation
   "Auto-discover source schema, convert it to PostgreSQL, migrate the data
    from the source definition to PostgreSQL for all the discovered
    items (tables, collections, etc), then reset the PostgreSQL sequences
    created by SERIAL columns in the first step.

    The target tables are automatically discovered, the only-tables
    parameter allows to filter them out."))



(defgeneric prepare-pgsql-database (db-copy catalog
                                    &key
                                      truncate
                                      create-tables
                                      create-schemas
                                      drop-indexes
                                      set-table-oids
                                      materialize-views
                                      foreign-keys
                                      include-drop)
  (:documentation "Prepare the target PostgreSQL database."))

(defgeneric complete-pgsql-database (db-copy catalog pkeys
                                     &key
                                       foreign-keys
                                       create-indexes
                                       create-triggers
                                       reset-sequences)
  (:documentation "Alter load duties for database sources copy support."))


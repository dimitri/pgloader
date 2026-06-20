(ns pgloader.source.protocol)

(defprotocol Source
  (source-name [this]
    "Human-readable source name for progress reporting. String.")

  (catalog [this]
    "Return a sequence of TableSpec maps describing what to copy.
     Called once before any data transfer begins.
     Each map: {:source-table string
                :target-schema string
                :target-table  string
                :columns       [{:name :source-type :target-type :cast :nullable :default :extra}]
                :options       map}")

  (read-rows [this table-spec]
    "Return a lazy sequence of rows for table-spec.
     Each row is a vector of String-or-nil values, in column order.
     Nil = SQL NULL = will be written as \\N in COPY TEXT.
     Must be safe to call concurrently for different table-specs.
     Must NOT accumulate all rows in memory.")

  (read-query [this sql]
    "Execute an arbitrary SQL query against the source.
     Returns a map {:columns [...], :rows (lazy-seq-of-vecs)}.
     Columns are {:column-name str :column-type str}.
     Rows are vectors of String-or-nil values.
     Throws UnsupportedOperationException for sources that don't support arbitrary queries.")

  (partition-source [this table-spec n chunk-bytes]
    "Return a seq of n Source instances that together cover all rows of table-spec
     as disjoint, independently readable partitions, or nil if partitioning is not
     applicable (small table, no eligible PK, unsupported source type, etc.).
     chunk-bytes controls partition granularity (target bytes per chunk before
     distributing across n readers).
     Each returned source has its own DB connection and reads only its portion.")

  (close! [this]
    "Release all resources held by this source.
     Called once after all tables are copied or on error."))

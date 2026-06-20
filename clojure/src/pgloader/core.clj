(ns pgloader.core
  (:require [pgloader.source.protocol :refer [Source source-name catalog close! read-query partition-source]]
            [pgloader.source.mysql   :as mysql-source]
            [pgloader.source.csv    :as csv-source]
            [pgloader.source.copy   :as copy-source]
            [pgloader.source.sqlite :as sqlite-source]
            [pgloader.source.dbf    :as dbf-source]
            [pgloader.source.fixed  :as fixed-source]
            [pgloader.source.pgsql  :as pgsql-source]
            [pgloader.source.mssql  :as mssql-source]
            [pgloader.utils.archive :as archive]
            [pgloader.prefetch :as prefetch]
            [pgloader.ddl.common :as ddl]
            [pgloader.batch :as batch]
            [pgloader.cast  :as cast]
            [pgloader.transforms :as transforms]
            [pgloader.copy  :as copy]
            [pgloader.ddl.citus :as citus]
            [pgloader.load-file.ast :as ast]
            [pgloader.log     :as plog]
            [pgloader.stats   :as stats]
            [pgloader.summary :as summary]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [hugsql.core :as hugsql]
            [hugsql.adapter.next-jdbc :as hugsql-adapter])
  (:import [org.postgresql PGConnection]
           [java.sql Connection DriverManager]
           [java.io File]
           [java.util.concurrent Executors ExecutorService Future TimeUnit])
  (:require [clojure.tools.logging :as log]))

(set! *warn-on-reflection* true)

(hugsql/set-adapter! (hugsql-adapter/hugsql-adapter-next-jdbc))

(hugsql/def-db-fns "pgloader/target/pgsql.sql")

(defn- table-exists?
  "Check if a PostgreSQL table already exists."
  [^Connection pg-conn schema table]
  (boolean (:exists (table-exists pg-conn {:schema schema :table table}))))

(defn postgres-connection
  [uri-map]
  (let [host (or (:host uri-map) "localhost")
        port (or (:port uri-map) 5432)
        db   (or (:db uri-map) (:database uri-map) "")
        url  (str "jdbc:postgresql://" host ":" port "/" db)
        props (java.util.Properties.)]
    (when (:user uri-map) (.setProperty props "user" (:user uri-map)))
    (when (:password uri-map) (.setProperty props "password" (:password uri-map)))
    (.setProperty props "ApplicationName" "pgloader")
    (.setProperty props "reWriteBatchedInserts" "true")
    (doto (DriverManager/getConnection url props)
      (.setAutoCommit false))))

(defn- pg-quote-if-needed
  "Double-quote an identifier only when PostgreSQL would require it.
   Unquoted PG identifiers must match [a-z_][a-z0-9_]* after lowercasing."
  [^String s]
  (if (re-matches #"[a-z_][a-z0-9_]*" s)
    s
    (str "\"" s "\"")))

(defn- table-stats-label
  "Return the summary label for a table, quoting only when needed."
  [schema table]
  (str (pg-quote-if-needed schema) "." (pg-quote-if-needed table)))

(defn- resolve-dbf-url
  "If source-map has a :url key (http/https), download + extract and return
   the path of the first .dbf file found in the archive.
   Adds :pre stats entries for 'download' and 'extract' when applicable."
  [source-map]
  (if-let [url (:url source-map)]
    (let [_ (stats/new-entry! :pre "download")
          dl-t0 (System/nanoTime)
          tmp-file (archive/http-fetch! url)
          dl-ns (- (System/nanoTime) dl-t0)
          _ (stats/update-entry! :pre "download"
              :rows 1 :bytes (.length ^File tmp-file) :total-nanos dl-ns)
          atype (archive/archive-type tmp-file)
          final-path
          (if atype
            (let [_ (stats/new-entry! :pre "extract")
                  ex-t0 (System/nanoTime)
                  tmp-dir (archive/expand! tmp-file)
                  ex-ns (- (System/nanoTime) ex-t0)
                  _ (stats/update-entry! :pre "extract" :rows 1 :total-nanos ex-ns)
                  dbf-files (archive/matching-files tmp-dir "(?i)\\.dbf$")]
              (when (empty? dbf-files)
                (throw (ex-info "No .dbf file found in archive" {:url url})))
              (log/info (str "Using DBF file: " (first dbf-files)))
              (.getAbsolutePath ^File (first dbf-files)))
            (.getAbsolutePath ^File tmp-file))]
      (assoc source-map :path final-path :url nil))
    source-map))

(defn- source-from-uri
  [uri-map table-spec with-options source-overrides & [decoding-as]]
  (case (:type uri-map)
    (:mysql :mariadb) (mysql-source/create-source uri-map
                        (merge (or table-spec {})
                               (select-keys uri-map [:table-pattern])
                               (select-keys with-options [:quote-ids :downcase-ids :snake-case-ids]))
                        decoding-as)
    :csv   (csv-source/create-source (merge (assoc uri-map :table-spec table-spec)
                                             with-options
                                             source-overrides))
    :copy  (copy-source/create-source (merge uri-map source-overrides))
    :sqlite (sqlite-source/create-source uri-map (or table-spec {})
                                         (select-keys with-options [:snake-case-ids :downcase-ids]))
    :dbf   (dbf-source/create-source (merge (resolve-dbf-url uri-map) source-overrides))
    :fixed (fixed-source/create-source (merge uri-map source-overrides
                                              (select-keys with-options [:select-columns :fixed-header])))
    :pgsql (pgsql-source/create-source uri-map (or table-spec {}))
    :mssql (mssql-source/create-source uri-map (or table-spec {}))
    (throw (ex-info "Unsupported source type" {:type (:type uri-map)}))))

(defn- normalize-table-spec
  [table-spec]
  (assoc table-spec
         :target-schema (or (:schema table-spec)
                            (:target-schema table-spec) "public")
         :target-table  (or (:table-name table-spec)
                            (:target-table table-spec))))

(defn- run-ddl-tx
  "Execute DDL statements for one table inside a single JDBC transaction.
   If any statement fails the transaction rolls back atomically —
   including the DROP TABLE — leaving the existing table intact."
  [^Connection pg-conn ddl-sqls]
  (try
    (doseq [sql ddl-sqls]
      (let [summary (first (clojure.string/split sql #"\("))]
        (log/debug (str "DDL: " summary)))
      (jdbc/execute! pg-conn [sql]))
    (.commit pg-conn)
    (catch Exception e
      (.rollback pg-conn)
      (throw e))))

(defn- apply-projection-using
  "Overlay compiled 'using' functions from TARGET TABLE projections onto cast-specs.
   projections is a vec of {:column-name :using expr-string}; columns is the
   table-spec columns vector. Returns an updated cast-specs vector."
  [cast-specs columns projections]
  (if (empty? projections)
    cast-specs
    (let [col-index (into {} (map-indexed (fn [i c] [(:column-name c) i]) columns))]
      (reduce (fn [specs {:keys [column-name using]}]
                (if-let [xfn (when using (transforms/compile-using-expr using))]
                  (if-let [idx (get col-index column-name)]
                    (assoc specs idx xfn)
                    specs)
                  specs))
              (vec cast-specs)
              projections))))

(defn- actual-table-columns
  "Query PostgreSQL for the actual column names of the target table."
  [^java.sql.Connection pg-conn schema table]
  (try
    (into #{} (map :column_name) (table-columns pg-conn {:schema schema :table table}))
    (catch Exception _ #{})))

(defn- copy-table
  [source table-spec pg-conn cast-rule-maps projections]
  (let [ts (normalize-table-spec table-spec)
        ;; Filter declared columns to only those that actually exist in the target table.
        ;; This matches v3 behavior: columns listed in the load file that don't exist in
        ;; the table are silently skipped (data for those positions is dropped).
        actual-cols (actual-table-columns pg-conn (:target-schema ts) (:target-table ts))
        source-cols (:columns ts)
        [kept-cols kept-indices]
        (if (seq actual-cols)
          (let [pairs (keep-indexed
                        (fn [i c]
                          (let [n (or (:column-name c) (:name c))]
                            (when (contains? actual-cols n) [c i])))
                        source-cols)]
            [(mapv first pairs) (mapv second pairs)])
          [source-cols (vec (range (count source-cols)))])
        ts           (assoc ts :columns kept-cols)
        row-filter-fn (when (< (count kept-cols) (count source-cols))
                        (let [^ints idx-arr (int-array kept-indices)]
                          (fn [row]
                            (mapv #(nth row % nil) idx-arr))))
        cast-specs   (cast/resolve-specs cast-rule-maps kept-cols)
        cast-specs   (apply-projection-using cast-specs kept-cols projections)
        start (System/nanoTime)
        result (prefetch/copy-table! source ts pg-conn cast-specs row-filter-fn)
        elapsed (- (System/nanoTime) start)]
    (assoc result
           :table-name (:target-table ts)
           :total-nanos elapsed)))

(defn- exec-post-ddl!
  "Execute a sequence of DDL statements, each in its own transaction.
   Errors are logged as warnings and skipped (post-load DDL is non-fatal)."
  [^Connection pg-conn sqls label]
  (doseq [sql sqls]
    (try
      (jdbc/execute! pg-conn [sql])
      (.commit pg-conn)
      (catch Exception e
        (.rollback pg-conn)
        (log/warn (str label " failed (skipping): " (.getMessage e)))))))

(defn- materialize-views!
  "Execute MATERIALIZE VIEWS: run each view's query against the source
   and write the results to the target PostgreSQL table via COPY.
   The target tables are expected to already exist (e.g. created by BEFORE LOAD DO)."
  [^java.sql.Connection pg-conn source matviews schema]
  (doseq [{:keys [name query]} matviews]
    (let [sql      (or query (str "SELECT * FROM `" name "`"))
          _        (log/info (str "Materializing view: " name))
          {:keys [columns rows]} (read-query source sql)
          col-count (count columns)
          table-spec {:target-schema schema
                      :target-table  name
                      :columns       (mapv (fn [c] {:column-name (:column-name c)}) columns)}
          copy-sql-str (copy/copy-sql table-spec)]
      (try
        (loop [b (batch/make-batch 1000 (* 20 1024 1024))
               remaining rows
               total-rows (long 0)]
          (if-let [row (first remaining)]
            (let [row-bytes (copy/format-row-bytes
                             row (vec (repeat col-count nil)))
                  b' (batch/batch-add-row! b row-bytes)]
              (if (batch/batch-full? b')
                (do (batch/send-batch! pg-conn b' copy-sql-str)
                    (.commit pg-conn)
                    (recur (batch/make-batch 1000 (* 20 1024 1024))
                           (rest remaining)
                           (long (+ total-rows (:row-count b')))))
                (recur b' (rest remaining) total-rows)))
            (when (pos? (:row-count b))
              (batch/send-batch! pg-conn b copy-sql-str)
              (.commit pg-conn)
              (log/info (str "Materialized view " name ": "
                             (+ total-rows (:row-count b)) " rows")))))
        (catch Exception e
          (.rollback pg-conn)
          (log/error e (str "Failed to materialize view " name)))))))

(declare run-command)

(defn run-archive-command
  "Execute a LOAD ARCHIVE command:
   1. Fetch the archive (HTTP or file).
   2. Expand it to a temp directory.
   3. Run BEFORE LOAD DO against the archive-level target (if provided).
   4. For each sub-command, inject :archive-dir into the source map and run it.
   5. Run AFTER LOAD DO.
   6. Clean up the temp directory."
  [cmd opts]
  (let [src        (:source cmd)
        arc-url    (:url src)
        arc-path   (:path src)
        target-uri (get-in cmd [:target :target-uri])
        tmp-file   (if arc-url
                     (do (stats/new-entry! :pre "download")
                         (let [t0 (System/nanoTime)
                               f  (archive/http-fetch! arc-url)
                               ns (- (System/nanoTime) t0)]
                           (stats/update-entry! :pre "download"
                             :rows 1 :bytes (.length ^java.io.File f) :total-nanos ns)
                           f))
                     (java.io.File. ^String arc-path))
        tmp-dir    (let [_ (stats/new-entry! :pre "extract")
                         t0 (System/nanoTime)
                         d  (archive/expand! tmp-file)
                         ns (- (System/nanoTime) t0)]
                     (stats/update-entry! :pre "extract" :rows 1 :total-nanos ns)
                     d)]
    (log/info (str "Archive extracted to " tmp-dir))
    (try
      ;; Archive-level BEFORE LOAD DO
      (when (and target-uri (seq (:before-load cmd)))
        (let [^java.sql.Connection pg-conn (postgres-connection target-uri)]
          (try
            (doseq [sql (:before-load cmd)]
              (log/debug (str "BEFORE LOAD (archive): " (clojure.string/trim sql)))
              (next.jdbc/execute! pg-conn [sql]))
            (.commit pg-conn)
            (catch Exception e
              (.rollback pg-conn)
              (log/error e "BEFORE LOAD DO (archive) failed")
              (throw e))
            (finally (.close pg-conn)))))
      ;; Run each sub-command with :archive-dir injected into its source.
      ;; Pass :sub-command? true so run-command skips stats/clear! and print-summary —
      ;; the archive command owns the stats lifecycle and prints one combined summary.
      (doseq [sub-cmd (:commands cmd)]
        (let [injected (update sub-cmd :source assoc :archive-dir tmp-dir)]
          (run-command injected (assoc opts :sub-command? true))))
      ;; Archive-level AFTER LOAD DO
      (when (and target-uri (seq (:after-load cmd)))
        (let [^java.sql.Connection pg-conn (postgres-connection target-uri)]
          (try
            (doseq [sql (:after-load cmd)]
              (log/debug (str "AFTER LOAD (archive): " (clojure.string/trim sql)))
              (next.jdbc/execute! pg-conn [sql]))
            (.commit pg-conn)
            (catch Exception e
              (.rollback pg-conn)
              (log/error e "AFTER LOAD DO (archive) failed"))
            (finally (.close pg-conn)))))
      (finally
        (archive/delete-tree! tmp-dir)
        (when arc-url (.delete ^java.io.File tmp-file))))))

(defn- apply-db-defaults
  "Apply CL pgloader-compatible defaults for LOAD DATABASE commands.
   CL defaults all of these to true; v4 must match or it silently skips DDL."
  [with-options]
  (merge {:create-tables   true
          :create-indexes  true
          :foreign-keys    true
          :reset-sequences true}
         with-options))

(defn run-command
  [cmd opts]
  (if (= :archive (:load-type cmd))
    ;; ── LOAD ARCHIVE ─────────────────────────────────────────────────────────
    (do (run-archive-command cmd opts)
        (summary/print-summary (or (:debug opts) (:verbose opts) false))
        (when-let [summary-path (:summary opts)]
          (summary/write-summary summary-path (or (:debug opts) (:verbose opts) false))))
    ;; ── All other load types ──────────────────────────────────────────────────
    (let [source-uri  (:source cmd)
          target-uri  (get-in cmd [:target :target-uri])
        _           (log/debug (str "Connecting to PostgreSQL at " (:raw target-uri)))
        ^Connection pg-conn (postgres-connection target-uri)
        _           (log/info (str "Connected to PostgreSQL at " (:raw target-uri)))
        source-overrides (select-keys source-uri [:inline-data])
        commands-filters (:filters cmd)
        table-filter (when commands-filters
                       (let [inc-pats (seq (:including commands-filters))
                             exc-pats (seq (:excluding commands-filters))
                             inc-res (when inc-pats (mapv re-pattern inc-pats))
                             exc-res (when exc-pats (mapv re-pattern exc-pats))]
                         (fn [table-name]
                           (and (or (nil? inc-res) (some #(re-find % table-name) inc-res))
                                (or (nil? exc-res) (not (some #(re-find % table-name) exc-res)))))))
        table-spec  (when table-filter {:table-filter table-filter})
        _           (when-not (:sub-command? opts) (stats/clear!))
        source      (source-from-uri source-uri table-spec (:with-options cmd) source-overrides (:decoding-as cmd))
        verbose     (or (:debug opts) (:verbose opts) false)]
    (log/info "pgloader v4")
    (log/info "Source:" (source-name source))
    (log/info "Target:" (:raw target-uri))
    (let [opts-batch-rows (some-> (get (:with-options cmd) :batch-rows) (long))
          opts-batch-size (some-> (get (:with-options cmd) :batch-size) (Long/parseLong))
          opts-prefetch-rows (some-> (get (:with-options cmd) :prefetch-rows) (long))]
      (binding [copy/*batch-rows* (or opts-batch-rows copy/*batch-rows*)
                copy/*batch-size* (or opts-batch-size copy/*batch-size*)
                copy/*prefetch-queue-capacity* (or opts-prefetch-rows copy/*prefetch-queue-capacity*)]
        (try
          ;; Send MySQL-specific SET params to the MySQL source connection before catalog fetch
          (when (#{:mysql :mariadb} (:type source-uri))
            (when-let [mysql-params (seq (filter :is-mysql (:set-parameters cmd)))]
              (log/debug "Sending MySQL SET parameters to source connection")
              (mysql-source/execute-set-params! source mysql-params)))
          (let [_          (log/debug (str "Connecting to source: " (source-name source)))
                _          (log/info (str "Fetching catalog from " (source-name source)))
                fetch-t0   (System/nanoTime)
                cat        (catalog source)
                ;; If MATERIALIZE ALL VIEWS, append view catalog entries to table catalog.
                ;; For a named list, views WITHOUT an explicit SQL definition are added
                ;; to the catalog so the normal DDL+copy pipeline handles them.
                ;; Views WITH an explicit SQL definition are handled by materialize-views! below.
                cat        (cond
                             (= :all (:materialize-views cmd))
                             (do (log/info "MATERIALIZE ALL VIEWS: fetching view list")
                                 (into cat (case (:type source-uri)
                                             (:mysql :mariadb) (mysql-source/catalog-views source)
                                             :mssql            (mssql-source/catalog-views source)
                                             [])))

                             (sequential? (:materialize-views cmd))
                             (let [no-def-names (into #{}
                                                      (keep #(when (nil? (:query %)) (:name %)))
                                                      (:materialize-views cmd))]
                               (if (seq no-def-names)
                                 (do (log/info (str "MATERIALIZE VIEWS (named, no SQL def): "
                                                    (clojure.string/join ", " no-def-names)))
                                     (into cat
                                           (filter #(contains? no-def-names
                                                               (or (:source-table-name %) (:table-name %)))
                                                   (case (:type source-uri)
                                                     (:mysql :mariadb) (mysql-source/catalog-views source)
                                                     :mssql            (mssql-source/catalog-views source)
                                                     []))))
                                 cat))

                             :else cat)
                fetch-ns   (- (System/nanoTime) fetch-t0)
                _          (log/info (str "Found " (count cat) " tables/views in source catalog"))
                _          (stats/new-entry! :pre "fetch meta data")
                _          (stats/update-entry! :pre "fetch meta data"
                             :rows (if (#{:mysql :mariadb :pgsql :mssql} (:type source-uri))
                                     (count cat) 0)
                             :total-nanos fetch-ns)]
          ;; Execute PG SET parameters (skip MySQL-specific ones) — each SET is its own transaction
          (when-let [set-params (seq (remove :is-mysql (:set-parameters cmd)))]
            (log/debug "Executing PostgreSQL SET parameters")
            (doseq [param set-params]
                (let [sql (str "SET " (:var param) " TO '" (:value param) "'")]
                (log/debug (str "SET: " sql))
                (try
                  (jdbc/execute! pg-conn [sql])
                  (.commit pg-conn)
                  (catch Exception e
                    (.rollback pg-conn)
                    (log/warn (str "SET command failed: " (.getMessage e))))))))
          ;; Execute BEFORE LOAD DO statements — all in one transaction
          (when-let [before-cmds (:before-load cmd)]
            (log/debug "Executing BEFORE LOAD DO commands")
            (try
              (doseq [sql before-cmds]
                (log/debug (str "BEFORE LOAD: " (clojure.string/trim sql)))
                (jdbc/execute! pg-conn [sql]))
              (.commit pg-conn)
              (catch Exception e
                (.rollback pg-conn)
                (log/error e "BEFORE LOAD DO failed")
                (throw e))))
          (let [with-options      (cond-> (:with-options cmd)
                                     (= :database (:load-type cmd)) apply-db-defaults)
                  ;; Apply type overrides, identifier-case, alter-schema,
                  ;; and target-table overrides to catalog
                  cmd-target-table  (:target-table with-options)
                  cmd-target-schema (:target-schema with-options)
                  cast-rule-maps    (:cast-rules cmd)
                  cat (-> cat
                          (as-> c
                            (if table-filter
                              (filterv #(table-filter (:table-name %)) c)
                              c))
                          (as-> c
                            (mapv (fn [t]
                                    (update t :columns
                                            (fn [cols]
                                              ;; Inject table-name so column cast rules can match qualified names
                                              (let [tagged (mapv #(assoc % :table-name (:table-name t)) cols)]
                                                (cast/apply-type-overrides tagged cast-rule-maps)))))
                                  c))
                          (ddl/apply-identifier-case with-options)
                          (ddl/apply-alter-schema (:alter-schema cmd))
                          (ddl/apply-alter-table (:alter-table cmd))
                           (as-> c
                             (if cmd-target-table
                               (mapv #(assoc %
                                       :table-name cmd-target-table
                                       :schema (or cmd-target-schema (:schema %) "public"))
                                     c)
                               c))
                           (as-> c
                             (if (seq (:distribute-rules cmd))
                               (try
                                 (citus/augment-catalog c (:distribute-rules cmd)
                                                        :source-type (get-in cmd [:source :type]))
                                 (catch Exception e
                                   (log/error e "Citus catalog augmentation failed, continuing without distributed table backfill")
                                   c))
                               c))
                           ;; Expand ENUM/SET columns to named PostgreSQL ENUM types
                           ddl/add-enum-types)]
          ;; Truncate tables if requested — each table its own transaction
          (when (get with-options :truncate)
            (log/debug "Truncating tables")
            (doseq [t cat]
              (let [schema (or (:schema t) "public")
                    table  (:table-name t)
                    sql (str "TRUNCATE " (ddl/identifier-quote schema) "."
                              (ddl/identifier-quote table))]
                (try
                  (jdbc/execute! pg-conn [sql])
                  (.commit pg-conn)
                  (catch Exception e
                    (.rollback pg-conn)
                    (log/warn (str "TRUNCATE failed for " schema "." table ": " (.getMessage e))))))))
           ;; Execute MATERIALIZE VIEWS with explicit SQL definitions — run each user-provided
           ;; query against the source, write to the target table via COPY.
           ;; MATERIALIZE ALL VIEWS and named views without SQL defs are handled by the catalog.
           (when-let [matviews (when (sequential? (:materialize-views cmd))
                                 (seq (filter :query (:materialize-views cmd))))]
             (let [mv-schema (or (:db source-uri) "public")]
               (materialize-views! pg-conn source matviews mv-schema)))
           (log/info (str "Processing tables in this order: "
                         (clojure.string/join ", " (map :table-name cat))))
          (log/info "Preparing target PostgreSQL schema")
          (let [failed-tables (atom #{})
                table-oids    (atom {})
                preserve-index-names? (get with-options :preserve-index-names false)
                create-indexes? (or (get with-options :create-indexes false)
                                    (get with-options :reindex false))
                schema-only?  (get with-options :schema-only false)
                ;; workers: parallel table copies; default 4 for DB sources, 8 for file sources (v3 defaults)
                workers       (or (get with-options :workers)
                                  (if (#{:mysql :mariadb :pgsql :mssql :sqlite} (:type source-uri)) 4 8))
                ;; multiple readers per table: opt-in; concurrency = N readers per table
                multiple-readers? (get with-options :multiple-readers false)
                concurrency   (long (or (get with-options :concurrency) 1))
                chunk-bytes   (long (or (get with-options :chunk-size) (* 50 1024 1024)))
                all-idx-count (int (transduce (map #(count (:indexes %))) + 0 cat))
                max-par-idx   (int (max 1 (or (get with-options :max-parallel-create-index)
                                               all-idx-count)))
                ;; Index executor: each table's indexes are submitted as soon as that
                ;; table's COPY finishes, so index builds overlap with subsequent copies.
                idx-executor  (when (and create-indexes? (pos? all-idx-count))
                                (Executors/newFixedThreadPool max-par-idx))
                idx-futures   (atom [])
                idx-wall-t0   (atom nil)]
          ;; drop schema — executed once per schema before per-table DDL
          (when (and (get with-options :drop-schema false)
                     (not (get with-options :data-only false)))
            (let [schemas (distinct (map #(or (:schema %) "public") cat))]
              (doseq [schema schemas]
                (log/info (str "Dropping schema " schema " CASCADE"))
                (try
                  (jdbc/execute! pg-conn [(str "DROP SCHEMA IF EXISTS "
                                               (ddl/identifier-quote schema) " CASCADE")])
                  (.commit pg-conn)
                  (catch Exception e
                    (.rollback pg-conn)
                    (log/warn (str "Failed to drop schema " schema ": " (.getMessage e))))))))
          ;; reindex — drop existing indexes before data load so they are rebuilt after
          (when (and (get with-options :reindex false)
                     (not (get with-options :schema-only false)))
            (doseq [t cat]
              (let [schema (or (:schema t) "public")
                    table  (:table-name t)]
                (when-let [idxs (seq (:indexes t))]
                  (doseq [idx idxs]
                    (let [idx-name (ddl/identifier-quote (:name idx))]
                      (try
                        (jdbc/execute! pg-conn [(str "DROP INDEX IF EXISTS " idx-name)])
                        (.commit pg-conn)
                        (catch Exception _
                          (.rollback pg-conn)))))))))
          ;; Phase 1: DDL for all tables — each table in its own transaction.
          ;; If a table's DDL fails, its transaction (including DROP TABLE)
          ;; is rolled back, leaving existing data intact.
           ;; Skip DDL if create-tables is false, create-no-tables, data-only
           ;; is set, or table already exists (e.g. created by BEFORE LOAD DO).
           (when (and (get with-options :create-tables false)
                      (not (get with-options :create-no-tables false))
                      (not (get with-options :data-only false)))
            (stats/new-entry! :pre "Create tables")
            (let [ddl-phase-start (System/nanoTime)]
            (doseq [[i t] (map-indexed vector cat)]
            (let [schema (or (:schema t) "public")
                  table  (:table-name t)
                  cols   (:columns t)
                  table-label (table-stats-label schema table)]
              (stats/new-entry! :data table-label)
              (log/info (str "COPY " table " [" (inc i) "/" (count cat) "]"))
              (log/debug (str "Creating DDL for table: " schema "." table))
              (try
                (let [ddl-start (System/nanoTime)
                      pk (:primary-key t)
                      enum-types (:enum-types t)
                      ddl-sqls (cond-> [(str "CREATE SCHEMA IF NOT EXISTS " (ddl/identifier-quote schema))]
                                 (not (get with-options :include-no-drop false))
                                 (conj (ddl/drop-table-if-exists-sql schema table))
                                 :always
                                 (conj (ddl/create-table-sql schema table cols (:table-comment t))))]
                  ;; Create ENUM/SET types before CREATE TABLE
                  (when (seq enum-types)
                    (exec-post-ddl! pg-conn (ddl/create-enum-types-sql enum-types)
                                    (str "ENUM TYPES for " table)))
                  (run-ddl-tx pg-conn ddl-sqls)
                  ;; Query table OID and create primary-key index with idx_{oid}_PRIMARY naming.
                  (when (seq pk)
                    (let [oid-row (table-oid pg-conn {:schema schema :table table})
                          oid     (:oid oid-row)]
                      (when oid
                        (swap! table-oids assoc (str schema "." table) oid)
                        (when-let [pk-sqls (ddl/create-primary-key-sql schema table pk oid)]
                          (run-ddl-tx pg-conn pk-sqls)))))
                  ;; Create ON UPDATE CURRENT_TIMESTAMP triggers
                  (when-let [trg-sqls (ddl/create-triggers-sql schema table cols)]
                    (exec-post-ddl! pg-conn trg-sqls (str "TRIGGER on " table)))
                  (stats/update-entry! :data table-label :rs-nanos (- (System/nanoTime) ddl-start)))
                (catch Exception e
                  (log/error (str "Failed to create " schema "." table ": " (.getMessage e)))
                  (stats/update-entry! :data table-label :errs 1)
                   (swap! failed-tables conj table)))))
            (stats/update-entry! :pre "Create tables"
              :rows (count cat)
              :total-nanos (- (System/nanoTime) ddl-phase-start))))
          ;; Create the index stats entry before Phase 2 so futures can update it.
          (when create-indexes?
            (stats/new-entry! :post "Create Indexes"))
          ;; Phase 2: COPY data only for tables whose DDL succeeded, using worker threads.
          ;; Each worker gets its own source connection and PostgreSQL connection so that
          ;; up to `workers` tables can be copied simultaneously (v3 workers= behavior).
          (when (not schema-only?)
          ;; Pre-create all per-table stats entries before spawning workers (prevents races).
          (doseq [t cat]
            (let [tl (table-stats-label (or (:schema t) "public") (:table-name t))]
              (when-not (some #(= (:label %) tl) (stats/entries :data))
                (stats/new-entry! :data tl))))
          (stats/new-entry! :post "COPY Wall-Clock Time")
          (let [copy-wall-t0    (System/nanoTime)
                mysql-set-params (when (#{:mysql :mariadb} (:type source-uri))
                                   (seq (filter :is-mysql (:set-parameters cmd))))
                workers-pool    (Executors/newFixedThreadPool (int workers))
                table-futs
                (mapv
                  (fn [[_i t]]
                    (.submit ^ExecutorService workers-pool
                      ^java.util.concurrent.Callable
                      (fn []
                        (let [worker-src (source-from-uri source-uri table-spec
                                           with-options source-overrides (:decoding-as cmd))
                              worker-pg  (postgres-connection target-uri)
                              pg-params  (seq (remove :is-mysql (:set-parameters cmd)))]
                          (when mysql-set-params
                            (mysql-source/execute-set-params! worker-src mysql-set-params))
                          (when pg-params
                            (doseq [param pg-params]
                              (try
                                (jdbc/execute! worker-pg [(str "SET " (:var param) " TO '" (:value param) "'")])
                                (.commit worker-pg)
                                (catch Exception e
                                  (.rollback worker-pg)
                                  (log/warn (str "Worker SET failed: " (.getMessage e)))))))
                          (try
                            (let [schema      (or (:schema t) "public")
                                  table       (:table-name t)
                                  cols        (:columns t)
                                  table-label (table-stats-label schema table)]
                              (when-not (@failed-tables table)
                                (let [;; Generated columns exist on the target (DDL emits
                                      ;; GENERATED ALWAYS AS) but must be excluded from COPY
                                      ;; since PostgreSQL cannot accept values for them.
                                      copy-cols (remove :generated-expression cols)
                                      ts (cond-> {:schema schema :table-name table :columns copy-cols
                                                  :source-schema (:source-schema t)
                                                  :source-table-name (:source-table-name t)}
                                           (:citus-read-sql t) (assoc :citus-read-sql (:citus-read-sql t)))
                                      disable-triggers? (get with-options :disable-triggers false)]
                                  (when disable-triggers?
                                    (try
                                      (jdbc/execute! worker-pg ["SET session_replication_role = replica"])
                                      (.commit worker-pg)
                                      (catch Exception e
                                        (.rollback worker-pg)
                                        (log/warn (str "Failed to disable triggers: " (.getMessage e))))))
                                  (try
                                    (let [parts (when (and multiple-readers? (> concurrency 1))
                                                  (seq (partition-source worker-src ts concurrency chunk-bytes)))
                                          result
                                          (if parts
                                            (do
                                              (log/info (str "COPY " table " using " (count parts) " parallel readers"))
                                              (let [part-exec (Executors/newVirtualThreadPerTaskExecutor)
                                                    part-futs
                                                    (mapv (fn [part-src]
                                                            (.submit ^ExecutorService part-exec
                                                              ^java.util.concurrent.Callable
                                                              (fn []
                                                                (let [part-pg (postgres-connection target-uri)]
                                                                  (when pg-params
                                                                    (doseq [param pg-params]
                                                                      (try
                                                                        (jdbc/execute! part-pg [(str "SET " (:var param) " TO '" (:value param) "'")])
                                                                        (.commit part-pg)
                                                                        (catch Exception _ (.rollback part-pg)))))
                                                                  (when disable-triggers?
                                                                    (try
                                                                      (jdbc/execute! part-pg ["SET session_replication_role = replica"])
                                                                      (.commit part-pg)
                                                                      (catch Exception _ (.rollback part-pg))))
                                                                  (try
                                                                    (copy-table part-src ts part-pg (:cast-rules cmd) (get with-options :projections []))
                                                                    (finally
                                                                      (close! part-src)
                                                                      (when disable-triggers?
                                                                        (try
                                                                          (jdbc/execute! part-pg ["SET session_replication_role = origin"])
                                                                          (.commit part-pg)
                                                                          (catch Exception _ (.rollback part-pg))))
                                                                      (.close ^java.sql.Connection part-pg)))))))
                                                          parts)]
                                                (try
                                                  (reduce
                                                    (fn [acc ^java.util.concurrent.Future f]
                                                      (let [r (.get f)]
                                                        {:rows-ok     (+ (:rows-ok acc) (:rows-ok r))
                                                         :rows-bad    (+ (:rows-bad acc) (:rows-bad r))
                                                         :bytes       (+ (:bytes acc) (:bytes r))
                                                         :rs-nanos    (max (:rs-nanos acc) (:rs-nanos r))
                                                         :ws-nanos    (max (:ws-nanos acc) (:ws-nanos r))
                                                         :total-nanos (max (:total-nanos acc) (:total-nanos r))
                                                         :reject-paths (:reject-paths r)}))
                                                    {:rows-ok 0 :rows-bad 0 :bytes 0 :rs-nanos 0 :ws-nanos 0 :total-nanos 0 :reject-paths nil}
                                                    part-futs)
                                                  (finally
                                                    (.shutdown ^ExecutorService part-exec)
                                                    (.awaitTermination ^ExecutorService part-exec Long/MAX_VALUE TimeUnit/NANOSECONDS)))))
                                            (copy-table worker-src ts worker-pg (:cast-rules cmd) (get with-options :projections [])))]
                                      (log/info (str "COPY " table " done: "
                                                     (:rows-ok result) " rows in "
                                                     (plog/fmt-duration (:total-nanos result))))
                                      (when (pos? (:rows-bad result))
                                        (log/warn (str table ": " (:rows-bad result) " rows rejected"
                                                       (when-let [p (-> result :reject-paths :reject-log)]
                                                         (str ", see " p)))))
                                      (let [reject-paths (:reject-paths result)]
                                        (stats/update-entry! :data table-label
                                          :read (:rows-ok result)
                                          :rows (:rows-ok result)
                                          :errs (:rows-bad result)
                                          :bytes (:bytes result)
                                          :rs-nanos (:rs-nanos result)
                                          :ws-nanos (:ws-nanos result)
                                          :total-nanos (:total-nanos result)
                                          :reject-data (:reject-data reject-paths)
                                          :reject-log (:reject-log reject-paths))))
                                    (catch Exception e
                                      (log/error e (str "Failed to copy table " schema "." table ": " (.getMessage e)))
                                      (stats/update-entry! :data table-label :errs 1))
                                    (finally
                                      ;; Reset triggers on worker-pg when single-reader path used it
                                      (when disable-triggers?
                                        (try
                                          (jdbc/execute! worker-pg ["SET session_replication_role = origin"])
                                          (.commit worker-pg)
                                          (catch Exception e2
                                            (.rollback worker-pg)
                                            (log/warn (str "Failed to re-enable triggers: " (.getMessage e2))))))))
                                  ;; Submit indexes after all readers for this table are done.
                                  (when (and idx-executor (seq (:indexes t)))
                                    (when (nil? @idx-wall-t0)
                                      (reset! idx-wall-t0 (System/nanoTime)))
                                    (let [oid  (when-not preserve-index-names?
                                                 (get @table-oids (str schema "." table)))
                                          sqls (ddl/create-indexes-sql schema table (:indexes t) oid)]
                                      (doseq [sql sqls]
                                        (swap! idx-futures conj
                                          (.submit ^ExecutorService idx-executor
                                            ^java.util.concurrent.Callable
                                            (fn []
                                              (let [conn (postgres-connection target-uri)]
                                                (try
                                                  (exec-post-ddl! conn [sql] (str "INDEX on " table))
                                                  (finally (.close ^Connection conn))))))))))
                                  )))
                            (finally
                              (close! worker-src)
                              (.close ^Connection worker-pg)))))))
                  (map-indexed vector cat))]
            (.shutdown ^ExecutorService workers-pool)
            (.awaitTermination ^ExecutorService workers-pool Long/MAX_VALUE TimeUnit/NANOSECONDS)
            (doseq [^Future f table-futs]
              (try (.get f) (catch Exception _)))
            (stats/update-entry! :post "COPY Wall-Clock Time"
              :total-nanos (- (System/nanoTime) copy-wall-t0))))
          ;; Schema-only: no COPY phase ran, so submit all indexes now.
          (when (and create-indexes? schema-only? idx-executor)
            (when (nil? @idx-wall-t0)
              (reset! idx-wall-t0 (System/nanoTime)))
            (doseq [t cat]
              (let [schema (or (:schema t) "public")
                    table  (:table-name t)]
                (when-let [idxs (seq (:indexes t))]
                  (let [oid  (when-not preserve-index-names?
                               (get @table-oids (str schema "." table)))
                        sqls (ddl/create-indexes-sql schema table idxs oid)]
                    (doseq [sql sqls]
                      (swap! idx-futures conj
                        (.submit ^ExecutorService idx-executor
                          ^java.util.concurrent.Callable
                          (fn []
                            (let [conn (postgres-connection target-uri)]
                              (try
                                (exec-post-ddl! conn [sql] (str "INDEX on " table))
                                (finally (.close ^Connection conn)))))))))))))
          ;; Await completion of all parallel index builds (overlapped with copy above).
          (when create-indexes?
            (when idx-executor
              (log/info "Waiting for parallel index builds to complete")
              (.shutdown ^ExecutorService idx-executor)
              (.awaitTermination ^ExecutorService idx-executor Long/MAX_VALUE TimeUnit/NANOSECONDS)
              (doseq [^Future f @idx-futures]
                (try (.get f) (catch Exception _))))
            (stats/update-entry! :post "Create Indexes"
              :rows (count @idx-futures)
              :total-nanos (- (System/nanoTime) (or @idx-wall-t0 (System/nanoTime)))))
          (when (get with-options :foreign-keys false)
            (log/info "Creating foreign keys")
            (stats/new-entry! :post "Create Foreign Keys")
            (let [start (System/nanoTime)
                  n     (atom 0)]
              (doseq [t cat]
                (let [schema (or (:schema t) "public")
                      table  (:table-name t)]
                  (when-let [fks (seq (:fkeys t))]
                    (exec-post-ddl! pg-conn
                                    (ddl/create-fkeys-sql schema table fks)
                                    (str "FK on " table))
                    (swap! n + (count fks)))))
              (stats/update-entry! :post "Create Foreign Keys"
                :rows @n :total-nanos (- (System/nanoTime) start))))
          (when (get with-options :reset-sequences false)
            (log/info "Resetting sequences")
            (stats/new-entry! :post "Reset Sequences")
            (let [start (System/nanoTime)
                  n     (atom 0)]
              (doseq [t cat]
                (let [schema (or (:schema t) "public")
                      table  (:table-name t)]
                  (when-let [seqs (seq (ddl/reset-sequences-sql schema table (:columns t)))]
                    (exec-post-ddl! pg-conn seqs (str "SEQUENCE for " table))
                    (swap! n inc))))
              (stats/update-entry! :post "Reset Sequences"
                :rows @n :total-nanos (- (System/nanoTime) start)))))
        ;; Execute AFTER LOAD DO statements — all in one transaction
        (when-let [after-cmds (:after-load cmd)]
          (log/debug "Executing AFTER LOAD DO commands")
          (try
            (doseq [sql after-cmds]
              (log/debug (str "AFTER LOAD: " (clojure.string/trim sql)))
              (jdbc/execute! pg-conn [sql]))
            (.commit pg-conn)
            (catch Exception e
              (.rollback pg-conn)
              (log/error e "AFTER LOAD DO failed"))))
        ;; ── Citus distribution ────────────────────────────────────────────
        (when-let [rules (seq (citus/expand-distribute-rules cat (:distribute-rules cmd)))]
          (log/info (str "Applying " (count rules) " Citus distribution rule(s)"))
          (let [schema-by-table (into {} (map (juxt :table-name :schema) cat))]
            (doseq [rule rules]
              (let [tgt-schema (or (get schema-by-table (:table rule))
                                   (get-in cmd [:target :schema])
                                   "public")]
                (try
                  (citus/execute-distribute! pg-conn (assoc rule :schema tgt-schema))
                  (.commit pg-conn)
                  (log/info (str "  " (:table rule)
                                 (if (= :reference (:type rule))
                                   " → reference table"
                                   (str " → distributed on " (:using rule)))))
                  (catch Exception e
                    (log/error e (str "Failed to distribute " (:table rule)
                                      ": " (.getMessage e)))
                    (.rollback pg-conn)))))))
        (log/info "Done copying all tables")))

      (finally
        (close! source)
        (.close pg-conn)))))
      (when-not (:sub-command? opts)
        (summary/print-summary verbose)
        (when-let [summary-path (:summary opts)]
          (summary/write-summary summary-path verbose))))))

(defn run-inline
  [source-uri target-uri opts]
  (let [cmd (ast/map->LoadCommand
              {:load-type :database
               :source source-uri
               :target {:type :pgsql :target-uri target-uri}
               :with-options {}
               :set-parameters nil
               :cast-rules nil
               :filters nil
               :alter-schema nil
               :alter-table nil
                :before-load nil
                :after-load nil
                :materialize-views nil
                :distribute-rules nil
                :commands nil})]
    (run-command cmd opts)))

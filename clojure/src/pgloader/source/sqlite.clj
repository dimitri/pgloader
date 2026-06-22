(ns pgloader.source.sqlite
  (:require [pgloader.source.protocol :refer [Source]]
            [hugsql.core :as hugsql]
            [next.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:import [java.sql Connection DriverManager ResultSet]))

(set! *warn-on-reflection* true)

(hugsql/def-db-fns "pgloader/source/sqlite.sql")

(defn- sqlite-connection [path]
  (DriverManager/getConnection (str "jdbc:sqlite:" path)))

(defn- pragma! [^java.sql.Connection conn sql-str]
  (jdbc/execute! conn [sql-str]))

(defn- list-columns [^java.sql.Connection conn table-name]
  (pragma! conn (str "PRAGMA table_info(`" table-name "`)")))

(defn- list-fkeys [^java.sql.Connection conn table-name]
  (pragma! conn (str "PRAGMA foreign_key_list(`" table-name "`)")))

(defn- list-table-indexes [^java.sql.Connection conn table-name]
  (pragma! conn (str "PRAGMA index_list(`" table-name "`)")))

(defn- list-index-cols [^java.sql.Connection conn index-name]
  (pragma! conn (str "PRAGMA index_info(`" index-name "`)")))

(defn- sqlite-function-default?
  "Return true if a default value is a SQLite-specific function that has no
   meaning in PostgreSQL (e.g. strftime, datetime, julianday)."
  [default]
  (when default
    (re-find #"(?i)\b(strftime|datetime|julianday|unixepoch)\s*\("
             (str default))))

(defn- extract-balanced [^String s start]
  "Extract the content between balanced parentheses starting at index `start`
   (which must point at the opening '('). Returns the inner string or nil."
  (loop [i (inc start) depth 1 ^StringBuilder buf (StringBuilder.)]
    (cond
      (>= i (count s))        nil
      (= (.charAt s i) \()    (recur (inc i) (inc depth) (.append buf (.charAt s i)))
      (= (.charAt s i) \))    (if (= depth 1)
                                (str buf)
                                (recur (inc i) (dec depth) (.append buf (.charAt s i))))
      :else                   (recur (inc i) depth (.append buf (.charAt s i))))))

(defn- generated-column-expressions
  "Return a map of {column-name → expression} for GENERATED ALWAYS AS columns
   by parsing the raw CREATE TABLE SQL from sqlite_master."
  [^java.sql.Connection conn table-name]
  (if-let [sql (:sql (get-create-table conn {:table table-name}))]
    (let [pattern (re-pattern "(?i)\\b(\\w+)\\b[^,\\n]*GENERATED\\s+ALWAYS\\s+AS\\s*\\(")]
      (into {}
            (keep (fn [[match col-name]]
                    (let [paren-pos (+ (.indexOf ^String sql match)
                                       (- (count match) 1))] ; position of '('
                      (when-let [expr (extract-balanced sql paren-pos)]
                        [col-name (str/trim expr)])))
                  (re-seq pattern sql))))
    {}))

(defn- detect-autoincrement
  [^java.sql.Connection conn table-name col-name pk-id]
  (when (pos? pk-id)
    (or
     (try (pos? (:seq (find-sequence conn {:table table-name}))) (catch Exception _ false))
     (when-let [sql (:sql (get-create-table conn {:table table-name}))]
       (let [upper (str/upper-case sql)]
         (str/includes? upper (str/upper-case (str col-name " INTEGER"))))))))

(def ^:private sqlite-type-map
  {;; text-affinity types
   "character"        "text"
   "varchar"          "text"
   "nvarchar"         "text"
   "char"             "text"
   "nchar"            "text"
   "clob"             "text"
   "text"             "text"
   ;; integer-affinity types — sized aliases (int4, int8) are exact entries
   ;; so they are not shadowed by the shorter "int" prefix
   "integer"          "bigint"
   "int"              "integer"
   "int2"             "smallint"
   "int4"             "integer"
   "int8"             "bigint"
   "tinyint"          "smallint"
   "smallint"         "smallint"
   "mediumint"        "integer"
   "bigint"           "bigint"
   "long"             "bigint"
   ;; blob/binary — byte[] is used by Hibernate/some ORMs (#1231)
   "blob"             "bytea"
   "byte"             "bytea"
   "byte[]"           "bytea"
   ;; real-affinity types
   "real"             "double precision"
   "float"            "double precision"
   "float4"           "real"
   "float8"           "double precision"
   "double"           "double precision"
   "double precision" "double precision"
   "numeric"          "numeric"
   "decimal"          "numeric"
   "number"           "numeric"
   ;; date/time and other types
   "boolean"          "boolean"
   "datetime"         "timestamptz"
   "timestamp"        "timestamptz"
   "timestamptz"      "timestamptz"
   "date"             "date"
   "time"             "time"})

(defn- sqlite-type->pg
  "Map a raw SQLite declared type name to a PostgreSQL type name.
   Normalises case then does an exact lookup followed by a longest-prefix
   match so that 'int' does not shadow 'integer', 'int4', 'int8', etc."
  [type-name]
  (let [lower (str/lower-case type-name)]
    (or (get sqlite-type-map lower)
        (some (fn [[k v]]
                (when (str/starts-with? lower k) v))
              (sort-by (comp - count key) sqlite-type-map))
        "text")))

(defn- apply-sqlite-identifier-case
  "Apply identifier case to a SQLite name, matching v3 behavior."
  [^String name id-case]
  (case id-case
    :snake-case-ids (-> name
                        (str/replace #"([a-z])([A-Z])" "$1_$2")
                        (str/replace #"([A-Z]+)([A-Z][a-z])" "$1_$2")
                        str/lower-case)
    (str/lower-case name)))

(deftype SQLiteSource
         [^java.sql.Connection conn
          ^String db-path
          id-case]  ; :snake-case-ids or nil (defaults to downcase)

  Source
  (source-name [_] (str "sqlite:" db-path))

  (catalog [_]
    (log/info (str "Fetching SQLite catalog from: " db-path))
    (let [tables (list-tables conn)]
      (->> tables
           (mapv (fn [t]
                   (let [src-table-name (:table_name t)
                         table-name (apply-sqlite-identifier-case src-table-name id-case)
                         cols (list-columns conn src-table-name)
                         gen-exprs (generated-column-expressions conn src-table-name)
                         pk-cids (set (keep (fn [c]
                                              (when (pos? (:pk c)) (:cid c)))
                                            cols))
                         pkey-cols (mapv :name (filter #(pos? (:pk %)) cols))
                         raw-idx (list-table-indexes conn src-table-name)
                         idxes (mapv (fn [idx]
                                       (let [idx-cols (list-index-cols conn (:name idx))]
                                         {:name (:name idx)
                                          :unique (= 1 (:unique idx))
                                          :columns (mapv :name idx-cols)}))
                                     raw-idx)
                         fks (list-fkeys conn src-table-name)]
                     {:table-name table-name
                      :source-table-name src-table-name
                      :schema "public"  ; match v3 behavior: SQLite tables land in "public" schema
                      :columns (mapv (fn [c]
                                       (let [col-type (:type c)
                                             is-pk (pos? (:pk c))
                                             ai (detect-autoincrement conn src-table-name (:name c) (:pk c))
                                             pg-type (if (and ai is-pk)
                                                       "bigserial"
                                                       (sqlite-type->pg col-type))
                                             dflt (:dflt_value c)
                                             gen-expr (get gen-exprs (:name c))]
                                         (cond-> {:column-name (:name c)
                                                  :column-type pg-type
                                                  :is-nullable (zero? (:notnull c))
                                                  :column-default (cond
                                                                    (nil? dflt) nil
                                                                    (sqlite-function-default? dflt) "CURRENT_TIMESTAMP"
                                                                    :else dflt)
                                                  :key is-pk
                                                  :extra (when ai "auto_increment")}
                                           gen-expr (assoc :generated-expression gen-expr))))
                                     cols)
                      :primary-key pkey-cols
                      :indexes idxes
                      :fkeys (mapv (fn [fk]
                                     {:name (str "fk_" src-table-name "_" (:from fk))
                                      :columns [(:from fk)]
                                      :ftable (apply-sqlite-identifier-case (:table fk) id-case)
                                      :fcols [(:to fk)]
                                      :on-delete (:on_delete fk)
                                      :on-update (:on_update fk)})
                                   fks)})))
           (sort-by :table-name))))

  (read-rows [_ table-spec-entry]
    (let [{:keys [table-name source-table-name columns]} table-spec-entry
          src-tbl (or source-table-name table-name)
          col-list (str/join ", " (map #(str "\"" (:column-name %) "\"") columns))
          col-names (mapv #(str/lower-case (:column-name %)) columns)
          sql (str "SELECT " col-list " FROM \"" src-tbl "\"")
          _ (log/debug (str "SQLite read-rows: " sql))
          rs (jdbc/execute! conn [sql])]
      (map (fn [row]
             (let [row-map (into {} (map (fn [[k v]] [(str/lower-case (name k)) v]) row))]
               (mapv (fn [cn] (some-> (get row-map cn) str)) col-names)))
           rs)))

  (read-query [_ sql]
    (let [stmt (.prepareStatement conn sql)]
      (try
        (let [rs (.executeQuery stmt)
              meta (.getMetaData rs)
              n (.getColumnCount meta)
              cols (vec (map (fn [i]
                               {:column-name (.getColumnName meta i)
                                :column-type (.getColumnTypeName meta i)})
                             (range 1 (inc n))))]
          {:columns cols
           :rows
           ((fn thisfn []
              (when (.next rs)
                (lazy-seq
                 (cons (loop [i 1 result (transient [])]
                         (if (<= i n)
                           (recur (inc i)
                                  (conj! result
                                         (let [v (.getObject rs i)]
                                           (when v (str v)))))
                           (persistent! result)))
                       (thisfn))))))})
        (catch Exception e
          (log/error (str "Query failed: " sql " - " (.getMessage e)))
          (throw e)))))

  (partition-source [_ _ _ _] nil)

  (close! [this]
    (try (.close conn) (catch Exception _))))

(defn- list-view-names
  "Return a seq of view name strings from sqlite_master."
  [^java.sql.Connection conn]
  (->> (jdbc/execute! conn ["SELECT tbl_name FROM sqlite_master WHERE type = ? ORDER BY tbl_name" "view"])
       (mapv (fn [row] (val (first row))))))

(defn catalog-views
  "Return catalog entries for all SQLite views, in the same shape as
   entries returned by (catalog source).  PRAGMA table_info works on views
   just like tables, so the column-mapping logic is identical."
  [^SQLiteSource source]
  (let [conn   (.-conn source)
        id-case (.-id_case source)]
    (->> (list-view-names conn)
         (mapv (fn [src-name]
                 (let [table-name (apply-sqlite-identifier-case src-name id-case)
                       cols (list-columns conn src-name)]
                   {:table-name        table-name
                    :source-table-name src-name
                    :schema            "public"
                    :is-view           true
                    :columns           (mapv (fn [c]
                                               {:column-name    (:name c)
                                                :column-type    (sqlite-type->pg (or (:type c) "text"))
                                                :is-nullable    (zero? (:notnull c))
                                                :column-default (let [d (:dflt_value c)]
                                                                  (cond
                                                                    (nil? d) nil
                                                                    (sqlite-function-default? d) "CURRENT_TIMESTAMP"
                                                                    :else d))
                                                :key            (pos? (:pk c))
                                                :extra          nil})
                                             cols)
                    :primary-key       []
                    :indexes           []
                    :fkeys             []})))
         (sort-by :table-name))))

(defn create-source
  [uri-map table-spec with-options]
  (let [conn (sqlite-connection (:path uri-map))
        id-case (cond
                  (:snake-case-ids with-options) :snake-case-ids
                  (:downcase-ids with-options)   :downcase-ids
                  :else                           :downcase-ids)]  ; v3 default: lowercase
    (->SQLiteSource conn (:path uri-map) id-case)))

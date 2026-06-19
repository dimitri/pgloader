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

(defn- detect-autoincrement
  [^java.sql.Connection conn table-name col-name pk-id]
  (when (pos? pk-id)
    (or
     (try (pos? (:seq (find-sequence conn {:table table-name}))) (catch Exception _ false))
     (when-let [sql (:sql (get-create-table conn {:table table-name}))]
       (let [upper (str/upper-case sql)]
         (str/includes? upper (str/upper-case (str col-name " INTEGER"))))))))

(def ^:private sqlite-type-map
  {"integer" "bigint"
   "int" "integer"
   "tinyint" "smallint"
   "smallint" "smallint"
   "mediumint" "integer"
   "bigint" "bigint"
   "text" "text"
   "varchar" "text"
   "nvarchar" "text"
   "char" "text"
   "nchar" "text"
   "clob" "text"
   "blob" "bytea"
   "real" "double precision"
   "float" "double precision"
   "double" "double precision"
   "double precision" "double precision"
   "numeric" "numeric"
   "decimal" "numeric"
   "boolean" "boolean"
   "datetime" "timestamptz"
   "timestamp" "timestamptz"
   "timestamptz" "timestamptz"
   "date" "date"
   "time" "time"})

(defn- sqlite-type->pg [type-name]
  (let [lower (str/lower-case type-name)]
    (or (some (fn [[k v]] (when (str/starts-with? lower k) v)) sqlite-type-map)
        "text")))

(defn- apply-sqlite-identifier-case
  "Apply identifier case to a SQLite name.
   v3 lowercases table/column names by default; snake_case also just lowercases
   (v3 does not split CamelCase for SQLite — only MySQL does)."
  [name _id-case]
  (str/lower-case name))

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
                      :schema "main"  ; SQLite default schema is "main" (matches v3 behavior for ALTER SCHEMA)
                       :columns (mapv (fn [c]
                                        (let [col-type (:type c)
                                              is-pk (pos? (:pk c))
                                              ai (detect-autoincrement conn src-table-name (:name c) (:pk c))
                                              pg-type (if (and ai is-pk)
                                                        "bigserial"
                                                        (sqlite-type->pg col-type))
                                              dflt (:dflt_value c)]
                                          {:column-name (:name c)
                                           :column-type pg-type
                                           :is-nullable (zero? (:notnull c))
                                           :column-default (when-not (sqlite-function-default? dflt) dflt)
                                           :key is-pk
                                           :extra (when ai "auto_increment")}))
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

  (close! [this]
    (try (.close conn) (catch Exception _))))

(defn create-source
  [uri-map table-spec with-options]
  (let [conn (sqlite-connection (:path uri-map))
        id-case (cond
                  (:snake-case-ids with-options) :snake-case-ids
                  (:downcase-ids with-options)   :downcase-ids
                  :else                           :downcase-ids)]  ; v3 default: lowercase
    (->SQLiteSource conn (:path uri-map) id-case)))

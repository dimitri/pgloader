(ns pgloader.source.mssql
  (:require [pgloader.source.protocol :refer [Source]]
            [hugsql.core :as hugsql]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [clojure.tools.logging :as log])
  (:import [java.sql Connection DriverManager ResultSet]))

(set! *warn-on-reflection* true)

(hugsql/def-db-fns "pgloader/source/mssql.sql")

(def ^:private mssql-type-map
  {"int" "integer"
   "bigint" "bigint"
   "smallint" "smallint"
   "tinyint" "smallint"
   "bit" "boolean"
   "decimal" "numeric"
   "numeric" "numeric"
   "money" "numeric"
   "smallmoney" "numeric"
   "float" "double precision"
   "real" "real"
   "char" "text"
   "nchar" "text"
   "varchar" "text"
   "nvarchar" "text"
   "text" "text"
   "ntext" "text"
   "xml" "xml"
   "datetime" "timestamptz"
   "datetime2" "timestamptz"
   "smalldatetime" "timestamptz"
   "datetimeoffset" "timestamptz"
   "date" "date"
   "time" "time"
   "varbinary" "bytea"
   "binary" "bytea"
   "image" "bytea"
   "uniqueidentifier" "uuid"
   "hierarchyid" "bytea"
   "geography" "bytea"
   "geometry" "bytea"})

(defn- mssql-type->pg [type-name]
  (let [lower (str/lower-case type-name)]
    (get mssql-type-map lower "text")))

(defn- connection
  [uri-map]
  (let [host (or (:host uri-map) "localhost")
        port (or (:port uri-map) 1433)
        db   (or (:db uri-map) "")
        url  (or (:jdbc-url uri-map)
                 (str "jdbc:sqlserver://" host ":" port ";databaseName=" db ";encrypt=false"))
        props (java.util.Properties.)]
    (when (:user uri-map) (.setProperty props "user" (:user uri-map)))
    (when (:password uri-map) (.setProperty props "password" (:password uri-map)))
    (.setProperty props "ApplicationName" "pgloader-source")
    (DriverManager/getConnection url props)))

(defn- translate-index-filter
  "Convert a SQL Server index filter expression to PostgreSQL syntax.
   Replaces [bracket]-quoted identifiers with \"double-quoted\" identifiers;
   the rest of the SQL Server predicate syntax (IS NULL, comparisons, etc.)
   is valid PostgreSQL as-is."
  [^String filter-def]
  (when filter-def
    (str/replace filter-def #"\[([^\]]+)\]" "\"$1\"")))

(defn- build-index-cols
  [index-rows]
  (let [grouped (group-by :index_name index-rows)]
    (mapv (fn [[idx-name rows]]
            (let [filter-pg (translate-index-filter (-> rows first :filter_definition))]
              (cond-> {:name    idx-name
                       :unique  (-> rows first :is_unique boolean)
                       :columns (mapv :column_name rows)}
                filter-pg (assoc :where filter-pg))))
          grouped)))

(defn- parse-fk-rules
  [fks]
  (let [grouped (group-by :constraint_name fks)]
    (mapv (fn [[cname rows]]
            {:name cname
             :columns (mapv :column_name rows)
             :ftable (-> rows first :foreign_table_name)
             :fcols (mapv :foreign_column_name rows)
             :on-delete (-> rows first :delete_rule)
             :on-update (-> rows first :update_rule)})
          grouped)))

(deftype MSSQLSource
  [^Connection conn
   ^String source-name-str]

  Source
  (source-name [_] source-name-str)

  (catalog [_]
    (log/info "Fetching table list from MS SQL Server")
    (let [ts (try
               (tables conn)
               (catch Exception e
                 (log/error (str "Failed to list tables: " (.getMessage e)))
                 []))]
      (log/debug (str "Fetched schema for " (count ts) " tables"))
      (->> ts
           (mapv (fn [t]
                   (let [schema (:table_schema t)
                         table-name (:table_name t)
                         cols (table-columns conn {:schema schema :table table-name})
                         pkeys (table-pkeys conn {:schema schema :table table-name})
                         idxes (table-indexes conn {:schema schema :table table-name})
                         fks   (table-fkeys conn {:schema schema :table table-name})
                         ext-props (try
                                     (table-extended-props conn {:schema schema :table table-name})
                                     (catch Exception _ []))
                         table-comment (not-empty (:comment (first (filter #(zero? (:minor_id %)) ext-props))))
                         col-comments (into {} (keep (fn [ep]
                                                       (when (pos? (:minor_id ep))
                                                         [(:column_name ep) (:comment ep)]))
                                                     ext-props))]
                     {:table-name table-name
                      :schema (if (= schema "dbo") "public" schema)
                      :source-schema schema
                      :table-comment table-comment
                      :columns (mapv (fn [c]
                                       (let [col-type (:data_type c)
                                             is-identity (= 1 (:is_identity c))
                                             is-pk (some #(= (:column_name c) %) (mapv :column_name pkeys))
                                             ai (and is-identity is-pk)]
                                         {:column-name (:column_name c)
                                          :column-type (if ai
                                                          "bigserial"
                                                          (mssql-type->pg col-type))
                                          :is-nullable (= "YES" (:is_nullable c))
                                          :column-default (when-not ai (:column_default c))
                                          :extra (when ai "auto_increment")
                                          :column-comment (not-empty (get col-comments (:column_name c)))}))
                                     cols)
                      :primary-key (mapv :column_name pkeys)
                      :indexes (build-index-cols idxes)
                      :fkeys (parse-fk-rules fks)})))
           (sort-by :table-name))))

  (read-rows [_ table-spec-entry]
    (let [{:keys [table-name columns schema]} table-spec-entry
          col-names     (mapv :column-name columns)
          col-list      (str/join ", " (map #(str "[" % "]") col-names))
          source-schema (or (:source-schema table-spec-entry) schema)
          sql           (str "SELECT " col-list " FROM [" source-schema "].[" table-name "]")
          _             (log/debug (str "MSSQL read-rows: " sql))
          stmt          (.prepareStatement conn sql)]
      (try
        (.setFetchSize stmt 1000)
        (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
        (let [rs   (.executeQuery stmt)
              meta (.getMetaData rs)
              n    (.getColumnCount meta)]
          ((fn thisfn []
             (when (try (.next rs)
                        (catch Exception e
                          (log/warn (str "MSSQL row advance error in " table-name ": " (.getMessage e)))
                          false))
               (lazy-seq
                 (cons (loop [i 1 result (transient [])]
                         (if (<= i n)
                           (recur (inc i)
                                  (conj! result
                                    ;; Per-column error recovery: on encoding or conversion errors
                                    ;; substitute nil and continue (mirrors v3 restart behaviour).
                                    (try
                                      (let [v (.getObject rs i)]
                                        (when v (str v)))
                                      (catch Exception e
                                        (log/warn (str "MSSQL column " i " read error in "
                                                       table-name " (substituting NULL): "
                                                       (.getMessage e)))
                                        nil))))
                           (persistent! result)))
                       (thisfn)))))))
        (catch Exception e
          (log/error (str "Query failed: " sql " - " (.getMessage e)))
          (throw e)))))

  (read-query [_ sql]
    (let [stmt (.prepareStatement conn sql)]
      (try
        (.setFetchSize stmt 1000)
        (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
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

(defn catalog-views
  "Return catalog entries for all user views in the MS SQL database,
   using the same structure as table entries from catalog.
   Used for MATERIALIZE ALL VIEWS."
  [^MSSQLSource src]
  (let [conn (.-conn src)]
    (->> (try (views conn) (catch Exception _ []))
         (mapv (fn [v]
                 (let [schema     (:table_schema v)
                       view-name  (:table_name v)
                       cols       (table-columns conn {:schema schema :table view-name})
                       pkeys      (table-pkeys   conn {:schema schema :table view-name})]
                   {:table-name  view-name
                    :schema      (if (= schema "dbo") "public" schema)
                    :source-schema schema
                    :is-view     true
                    :columns     (mapv (fn [c]
                                        {:column-name    (:column_name c)
                                         :column-type    (mssql-type->pg (:data_type c))
                                         :is-nullable    (= "YES" (:is_nullable c))
                                         :column-default (:column_default c)})
                                      cols)
                    :primary-key (mapv :column_name pkeys)
                    :indexes     []
                    :fkeys       []}))))))

(defn create-source
  [uri-map _table-spec]
  (let [conn (connection uri-map)]
    (->MSSQLSource conn (:raw uri-map))))

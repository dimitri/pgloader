(ns pgloader.source.mysql
  (:require [pgloader.source.protocol :refer [Source read-rows source-name]]
            [hugsql.core :as hugsql]
            [clojure.string :as str]
            [next.jdbc :as jdbc])
  (:import [java.sql Connection PreparedStatement ResultSet]
           [com.mysql.cj.jdbc MysqlDataSource]))

(set! *warn-on-reflection* true)

(hugsql/def-db-fns "pgloader/source/mysql.sql")

(deftype MySQLSource
  [^Connection conn
   ^String database
   ^String schema-name
   table-spec]

  Source
  (source-name [_] (str "mysql://" database "/" (:table-name table-spec)))

  (catalog [_]
    (let [ts (try
               (tables conn)
               (catch Exception _ []))]
      (->> ts
           (map (fn [t]
                  (let [table-name (:table_name t)
                        cols (columns conn
                                      {:schema schema-name
                                       :table table-name})
                        pkeys (table-pkeys conn
                                           {:schema schema-name
                                            :table table-name})]
                    {:table-name table-name
                     :schema     schema-name
                     :columns    (mapv (fn [c]
                                         {:column-name      (:column_name c)
                                          :column-type      (:column_type c)
                                          :is-nullable      (= "YES" (:is_nullable c))
                                          :column-default   (:column_default c)
                                          :extra            (:extra c)
                                          :column-key       (:column_key c)
                                          :ordinal-position (:ordinal_position c)})
                                       cols)
                     :primary-key (mapv :column_name pkeys)})))
           (filter (fn [t]
                     (if-let [filter-fn (:table-filter table-spec)]
                       (filter-fn (:table-name t))
                       true)))
           (sort-by :table-name))))

  (read-rows [_ table-spec-entry]
    (let [{:keys [table-name columns]} table-spec-entry
          col-names (if (seq columns)
                      (mapv :column-name columns)
                      ["*"])
          col-list  (if (= ["*"] col-names)
                      "*"
                      (str/join ", " (map #(str "`" % "`") col-names)))
          sql       (str "SELECT " col-list " FROM `" table-name "`")
          stmt      (.prepareStatement conn sql)
          _         (doto stmt
                      (.setFetchSize Integer/MIN_VALUE)
                      (.setFetchDirection ResultSet/FETCH_FORWARD))
          rs        (.executeQuery stmt)
          meta      (.getMetaData rs)
          n         (.getColumnCount meta)
          ;; Pre-compute column type names from JDBC metadata
          jdbc-types (vec (map (fn [i]
                                (.getColumnTypeName meta i))
                              (range 1 (inc n))))]
      ((fn thisfn []
         (when (.next rs)
           (lazy-seq
             (cons (loop [i 1
                          result (transient [])]
                     (if (<= i n)
                       (recur (inc i)
                              (conj! result
                                (let [v (.getObject rs i)
                                      jdbc-type (nth jdbc-types (dec i))
                                      col-type (nth columns (dec i) nil)
                                      raw-col-type (:column-type col-type)]
                                  (if (nil? v)
                                    nil
                                    (cond
                                      (instance? Boolean v) (if v "1" "0")
                                      (= "YEAR" jdbc-type)
                                      (str (if (instance? java.sql.Date v)
                                             (+ 1900 (.getYear ^java.sql.Date v))
                                             v))
                                      (instance? java.sql.Timestamp v)
                                      (str/replace (str v) #"\.0$" "")
                                       (and raw-col-type
                                            (str/starts-with? (str/lower-case raw-col-type) "set("))
                                       (str "{" v "}")
                                       (instance? (class (byte-array 0)) v)
                                       (let [ba ^bytes v
                                             sb (StringBuilder. (inc (* 2 (alength ba))))]
                                         (.append sb \X)
                                         (doseq [b ba]
                                           (.append sb (format "%02x" (bit-and (int b) 0xff))))
                                         (.toString sb))
                                       :else (str v))))))
                       (persistent! result)))
                   (thisfn))))))))

  (close! [this]
    (try (.close conn) (catch Exception _))))

(defn connection
  "Create a MySQL JDBC connection."
  [uri-map]
  (let [ds (MysqlDataSource.)
        _ (.setServerName ds (:host uri-map))
        _ (.setPortNumber ds (int (:port uri-map)))
        _ (.setDatabaseName ds (:db uri-map))
        _ (when (:user uri-map) (.setUser ds (:user uri-map)))
        _ (when (:password uri-map) (.setPassword ds (:password uri-map)))
        _ (.setCharacterEncoding ds "UTF-8")
        _ (.setRewriteBatchedStatements ds true)
        _ (.setUseCursorFetch ds false)]
    (.getConnection ds)))

(defn create-source
  "Create a MySQLSource from a parsed load file AST."
  [uri-map table-spec]
  (let [conn (connection uri-map)]
    (->MySQLSource conn (:db uri-map) (:db uri-map) table-spec)))

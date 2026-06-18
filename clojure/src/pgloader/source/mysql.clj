(ns pgloader.source.mysql
  (:require [pgloader.source.protocol :refer [Source read-rows source-name]]
            [hugsql.core :as hugsql]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [clojure.tools.logging :as log])
  (:import [java.sql Connection PreparedStatement ResultSet]
           [com.mysql.cj.jdbc MysqlDataSource]))

(set! *warn-on-reflection* true)

(hugsql/def-db-fns "pgloader/source/mysql.sql")

(defn- split-index-columns
  "Split a comma-separated index column list, respecting parentheses.
   Expression columns are wrapped in parens by the SQL query, e.g.:
   'email,(lower(`name`)),age' → [\"email\" \"(lower(`name`))\" \"age\"]"
  [^String s]
  (when s
    (loop [chars (seq s) depth 0 current (StringBuilder.) result []]
      (if (empty? chars)
        (let [last (str/trim (.toString current))]
          (if (str/blank? last) result (conj result last)))
        (let [c (first chars)]
          (cond
            (= c \() (do (.append current c) (recur (rest chars) (inc depth) current result))
            (= c \)) (do (.append current c) (recur (rest chars) (dec depth) current result))
            (and (= c \,) (zero? depth))
            (let [part (str/trim (.toString current))]
              (recur (rest chars) depth (StringBuilder.) (conj result part)))
            :else (do (.append current c) (recur (rest chars) depth current result))))))))

(defn- translate-mysql-expression
  "Convert a MySQL expression index column (wrapped in parens) to PostgreSQL.
   Strips MySQL backtick-quoting of identifiers.
   e.g. '(lower(`email`))' → '(lower(email))'"
  [^String s]
  (str/replace s "`" ""))

(defn- apply-identifier-case
  "Apply identifier case transformation to a name string.
   option is one of :quote-ids, :downcase-ids, :snake-case-ids."
  [^String name option]
  (case option
    :downcase-ids (str/lower-case name)
    :snake-case-ids (-> name
                        (str/replace #"([a-z])([A-Z])" "$1_$2")
                        (str/replace #"([A-Z]+)([A-Z][a-z])" "$1_$2")
                        str/lower-case)
    name))

(defn- identifier-case-option
  "Return the identifier-case option from table-spec, or nil."
  [table-spec]
  (or (:quote-ids table-spec)
      (:downcase-ids table-spec)
      (:snake-case-ids table-spec)
      :quote-ids))

(defn- parse-mysql-server-version
  "Query the server once and return a map describing its variant and version.
   Returns {:variant :mysql/:mariadb :major-version N :version-string \"...\"}."
  [^Connection conn]
  (try
    (let [^java.sql.ResultSet rs (.executeQuery (.createStatement conn)
                                                "SELECT @@version, @@version_comment")]
      (if (.next rs)
        (let [ver     (.getString rs 1)
              comment (or (.getString rs 2) "")
              lower   (str/lower-case ver)
              mariadb? (or (str/includes? lower "mariadb")
                           (str/includes? (str/lower-case comment) "mariadb"))
              major   (try (Integer/parseInt (first (str/split ver #"[\.\-]")))
                           (catch Exception _ 0))]
          {:variant        (if mariadb? :mariadb :mysql)
           :major-version  major
           :version-string ver})
        {:variant :mysql :major-version 0 :version-string "unknown"}))
    (catch Exception e
      (log/warn (str "Could not detect MySQL server version: " (.getMessage e)))
      {:variant :mysql :major-version 0 :version-string "unknown"})))

(defn- geometry-type?
  "Return true if col-type is a MySQL spatial/geometry type."
  [^String col-type]
  (let [lower (str/lower-case (or col-type ""))]
    (some #(str/starts-with? lower %)
          ["geometry" "point" "linestring" "polygon"
           "multipoint" "multilinestring" "multipolygon" "geometrycollection"])))

(defn- decoding-as-charset
  "Return the charset for TABLE-NAME by matching against DECODING-AS rules,
   or nil if no rule matches."
  [table-name decoding-as-rules]
  (when (seq decoding-as-rules)
    (some (fn [{:keys [patterns encoding]}]
            (when (some (fn [pat]
                          (case (:type pat)
                            :regex (re-find (re-pattern (:value pat)) table-name)
                            :exact (= (:value pat) table-name)
                            nil))
                        patterns)
              encoding))
          decoding-as-rules)))

(deftype MySQLSource
  [^Connection conn
   ^String database
   ^String schema-name
   table-spec
   decoding-rules  ; seq of {:patterns [...] :encoding charset} or nil
   server-variant  ; {:variant :mysql/:mariadb :major-version N :version-string "..."}
   active-stmt     ; volatile! — current streaming PreparedStatement, or nil
   active-rs]      ; volatile! — current streaming ResultSet, or nil

  Source
  (source-name [_] (str "mysql://" database "/" (:table-name table-spec)))

  (catalog [_]
    (log/debug (str "CONNECTED TO mysql://" database "/" (:table-name table-spec)))
    (log/info "Fetching table list from MySQL")
    (let [ts (try
               (tables conn)
               (catch Exception _ []))
          id-case (identifier-case-option table-spec)]
      (log/debug (str "Fetched schema for " (count ts) " tables"))
      (let [mariadb? (= :mariadb (:variant server-variant))]
      (->> ts
           (map (fn [t]
                  (let [table-name (apply-identifier-case (:table_name t) id-case)
                        cols (columns conn
                                      {:schema schema-name
                                       :table (:table_name t)})
                        pkeys (table-pkeys conn
                                           {:schema schema-name
                                            :table (:table_name t)})
                        idxes (if mariadb?
                                (table-indexes-mariadb conn {:schema schema-name
                                                             :table (:table_name t)})
                                (table-indexes conn {:schema schema-name
                                                     :table (:table_name t)}))
                        fks   (table-fkeys conn
                                           {:schema schema-name
                                            :table (:table_name t)})]
                    {:table-name        table-name
                     :source-table-name (:table_name t)
                     :schema            schema-name
                     :table-comment     (not-empty (:table_comment t))
                    :columns    (mapv (fn [c]
                                          {:column-name      (apply-identifier-case (:column_name c) id-case)
                                           :column-type      (:column_type c)
                                           :is-nullable      (= "YES" (:is_nullable c))
                                           :column-default   (:column_default c)
                                           :extra            (:extra c)
                                           :column-key       (:column_key c)
                                           :ordinal-position (:ordinal_position c)
                                           :column-comment   (:column_comment c)})
                                        cols)
                     :primary-key (mapv #(apply-identifier-case % id-case) (mapv :column_name pkeys))
                     :indexes    (mapv (fn [idx]
                                         {:name    (:index_name idx)
                                          :unique  (zero? (:non_unique idx))
                                          :columns (mapv (fn [col]
                                                           (if (str/starts-with? col "(")
                                                             (translate-mysql-expression col)
                                                             (apply-identifier-case col id-case)))
                                                         (split-index-columns (:columns idx)))})
                                       idxes)
                     :fkeys      (mapv (fn [fk]
                                         {:name      (:constraint_name fk)
                                          :columns   (mapv #(apply-identifier-case % id-case)
                                                           (str/split (:cols fk) #","))
                                          :ftable    (apply-identifier-case (:ftable fk) id-case)
                                          :fcols     (mapv #(apply-identifier-case % id-case)
                                                           (str/split (:fcols fk) #","))
                                          :on-delete (:delete_rule fk)
                                          :on-update (:update_rule fk)})
                                       fks)})))
           (filter (fn [t]
                     (if-let [filter-fn (:table-filter table-spec)]
                       (filter-fn (:table-name t))
                       true)))
           (sort-by :table-name)))))

  (read-rows [_ table-spec-entry]
    (let [{:keys [table-name source-table-name columns citus-read-sql]} table-spec-entry
          ;; Apply per-table charset override if a DECODING rule matches.
          _ (when-let [charset (decoding-as-charset (or source-table-name table-name) decoding-rules)]
              (try
                (jdbc/execute! conn [(str "SET NAMES '" charset "'")])
                (log/info (str "SET NAMES '" charset "' for table " (or source-table-name table-name)))
                (catch Exception e
                  (log/warn (str "SET NAMES failed for " table-name ": " (.getMessage e))))))
          ;; Use source-table-name (original MySQL name) for the FROM clause —
          ;; MySQL on Linux is case-sensitive for table names.
          mysql-table (or source-table-name table-name)
          sql       (or citus-read-sql
                        (let [col-list  (if (seq columns)
                                          (str/join ", "
                                            (mapv (fn [col]
                                                    (let [cn (:column-name col)
                                                          ;; Use source-column-type (pre-cast) for geometry detection
                                                          ct (or (:source-column-type col) (:column-type col))]
                                                      (if (geometry-type? ct)
                                                        (str "ST_AsText(`" cn "`) AS `" cn "`")
                                                        (str "`" cn "`"))))
                                                  columns))
                                          "*")]
                          (str "SELECT " col-list " FROM `" mysql-table "`")))
          stmt      (.prepareStatement ^Connection conn sql)
          _         (doto ^PreparedStatement stmt
                      (.setFetchSize Integer/MIN_VALUE)
                      (.setFetchDirection ResultSet/FETCH_FORWARD))
          _         (vreset! active-stmt stmt)
          rs        (.executeQuery ^PreparedStatement stmt)
          _         (vreset! active-rs rs)
          meta      (.getMetaData rs)
          n         (.getColumnCount meta)
          ;; Pre-compute column type names from JDBC metadata
          jdbc-types (vec (map (fn [i]
                                (.getColumnTypeName meta i))
                              (range 1 (inc n))))]
      ((fn thisfn []
         (let [has-next (try (.next rs)
                             (catch Exception e
                               ;; MySQL threw during row streaming (e.g. data conversion error).
                               ;; Close the RS/stmt now so the connection is usable for next table.
                               (try (.close rs) (catch Exception _))
                               (try (.close stmt) (catch Exception _))
                               (vreset! active-rs nil)
                               (vreset! active-stmt nil)
                               (throw e)))]
         (when has-next
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
                                      (= "YEAR" jdbc-type)
                                      (str (if (instance? java.sql.Date v)
                                             (+ 1900 (.getYear ^java.sql.Date v))
                                             v))
                                      (instance? java.sql.Date v)
                                      (let [s (str v)]
                                        (if (re-matches #"0000[-/]00[-/]00.*" s) nil s))
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
                   (thisfn)))))))))

  (read-query [_ sql]
    (let [stmt (.prepareStatement conn sql)
          _    (doto stmt
                 (.setFetchSize Integer/MIN_VALUE)
                 (.setFetchDirection ResultSet/FETCH_FORWARD))
          rs   (.executeQuery stmt)
          meta (.getMetaData rs)
          n    (.getColumnCount meta)
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
                    (thisfn))))))}))

  (close! [_]
    ;; Close any open streaming ResultSet and PreparedStatement first.
    ;; This is critical: if a table load fails mid-stream, the MySQL
    ;; connection refuses all further queries until the RS is closed.
    (try
      (when-let [^ResultSet rs @active-rs]
        (vreset! active-rs nil)
        (.close rs))
      (catch Exception _))
    (try
      (when-let [^PreparedStatement s @active-stmt]
        (vreset! active-stmt nil)
        (.close s))
      (catch Exception _))
    (try (.close ^Connection conn) (catch Exception _))))

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
        _ (.setUseCursorFetch ds false)
        ;; Convert zero dates (0000-00-00) to NULL instead of throwing
        _ (.setZeroDateTimeBehavior ds "CONVERT_TO_NULL")]
    (.getConnection ds)))

(defn catalog-views
  "Return catalog entries for all MySQL views, same structure as catalog entries
   for tables. Used for MATERIALIZE ALL VIEWS."
  [^MySQLSource src]
  (let [conn        (.-conn src)
        schema-name (.-schema_name src)
        table-spec  (.-table_spec src)
        id-case     (identifier-case-option table-spec)
        view-rows   (try
                      (views conn)
                      (catch Exception _ []))]
    (->> view-rows
         (mapv (fn [v]
                 (let [view-name  (:view_name v)
                       table-name (apply-identifier-case view-name id-case)
                       cols (columns conn {:schema schema-name :table view-name})]
                   {:table-name        table-name
                    :source-table-name view-name
                    :schema            schema-name
                    :is-view           true
                    :columns           (mapv (fn [c]
                                              {:column-name      (apply-identifier-case (:column_name c) id-case)
                                               :column-type      (:column_type c)
                                               :is-nullable      (= "YES" (:is_nullable c))
                                               :column-default   (:column_default c)
                                               :extra            (:extra c)
                                               :column-key       (:column_key c)
                                               :ordinal-position (:ordinal_position c)
                                               :column-comment   (:column_comment c)})
                                            cols)
                    :primary-key []
                    :indexes     []
                    :fkeys       []}))))))

(defn execute-set-params!
  "Execute MySQL-specific SET variable = 'value' statements on the MySQL connection.
   Called before catalog fetch so session settings take effect for the whole load."
  [^MySQLSource src mysql-params]
  (doseq [p mysql-params]
    (let [val (:value p)
          ;; Numeric values must not be quoted (MySQL 8.x rejects SET timeout = '120')
          quoted-val (if (re-matches #"-?\d+(\.\d+)?" val) val (str "'" val "'"))
          sql (str "SET " (:var p) " = " quoted-val)]
      (log/debug (str "MySQL SET: " sql))
      (try
        (jdbc/execute! (.-conn src) [sql])
        (catch Exception e
          (log/warn (str "MySQL SET failed: " (.getMessage e))))))))

(defn create-source
  "Create a MySQLSource from a parsed load file AST."
  [uri-map table-spec & [decoding-as-rules]]
  (let [conn    (connection uri-map)
        sv      (parse-mysql-server-version conn)
        _       (log/info (str "MySQL server: " (:version-string sv)
                               " (" (name (:variant sv)) ")"))
        ts (if-let [pat (:table-pattern table-spec)]
             (update table-spec :table-filter
                     (fn [existing]
                       (or existing
                           (let [re (re-pattern pat)]
                             #(re-find re %)))))
             table-spec)]
    (->MySQLSource conn (:db uri-map) (:db uri-map) ts decoding-as-rules sv (volatile! nil) (volatile! nil))))

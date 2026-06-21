(ns pgloader.source.mysql
  (:require [pgloader.source.protocol :refer [Source read-rows source-name partition-source]]
            [hugsql.core :as hugsql]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [clojure.tools.logging :as log])
  (:import [java.sql Connection DriverManager PreparedStatement ResultSet]))

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

(defn- mysql-gen-expr->pg
  "Convert a MySQL GENERATION_EXPRESSION to PostgreSQL syntax.
   Replaces backtick-quoted identifiers with double-quoted identifiers.
   Returns nil for empty/blank expressions."
  [^String expr]
  (when-not (str/blank? expr)
    (str/replace expr #"`([^`]+)`" "\"$1\"")))

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

(declare mysql-partition-source)

(defn- split-range
  "Divide [lo, hi) into chunks of size chunk-size.
   Returns a seq of [chunk-lo chunk-hi] pairs."
  [lo hi chunk-size]
  (when (and lo hi (< lo hi) (pos? chunk-size))
    (take-while (fn [[a _]] (< a hi))
                (iterate (fn [[_ b]] [b (min hi (+ b chunk-size))])
                         [lo (min hi (+ lo chunk-size))]))))

(defn- distribute
  "Round-robin distribute coll across n buckets.
   Returns a vector of n vectors (some may be empty)."
  [coll n]
  (let [buckets (mapv (fn [_] (transient [])) (range n))]
    (doseq [[i item] (map-indexed vector coll)]
      (conj! (nth buckets (mod i n)) item))
    (mapv persistent! buckets)))

(defn- text-col-type?
  "Return true for MySQL column types that hold character data and may contain
   embedded NUL bytes (0x00) which JDBC getString() silently truncates (#1573)."
  [^String raw-col-type]
  (when raw-col-type
    (let [lower (str/lower-case raw-col-type)]
      (some #(str/starts-with? lower %)
            ["char" "varchar" "tinytext" "mediumtext" "longtext" "text"]))))

(defn- convert-mysql-value
  "Convert a raw JDBC value from MySQL to a String suitable for COPY TEXT.
   For text columns, reads bytes directly and re-encodes as UTF-8 to preserve
   embedded NUL bytes that JDBC getString() would silently truncate (#1573)."
  [v jdbc-type col ^java.sql.ResultSet rs col-idx]
  (when (some? v)
    (let [raw-col-type (:column-type col)]
      (cond
        (instance? Boolean v)    (if v "1" "0")
        (= "YEAR" jdbc-type)     (str (if (instance? java.sql.Date v)
                                        (+ 1900 (.getYear ^java.sql.Date v))
                                        v))
        (instance? java.sql.Timestamp v) (str/replace (str v) #"\.0$" "")
        (instance? java.sql.Date v)      (let [s (str v)]
                                           (when-not (re-matches #"0000[-/]00[-/]00.*" s) s))
        (and raw-col-type
             (str/starts-with? (str/lower-case raw-col-type) "set("))
        (str "{" v "}")
        (instance? (class (byte-array 0)) v)
        (let [ba ^bytes v
              sb (StringBuilder. (inc (* 2 (alength ba))))]
          (.append sb \X)
          (doseq [b ba] (.append sb (format "%02x" (bit-and (int b) 0xff))))
          (.toString sb))
        ;; Text columns: use getString so the JDBC driver applies charset
        ;; conversion correctly (#1573). getString preserves NUL bytes in
        ;; MySQL Connector/J; getObject may truncate at the first NUL.
        (and (instance? String v) (text-col-type? raw-col-type))
        (.getString rs (int col-idx))
        :else (str v)))))

(defn- mysql-select-sql
  "Build base SELECT SQL for a table given its column metadata and MySQL table name."
  [columns mysql-table]
  (let [col-list (if (seq columns)
                   (str/join ", "
                             (mapv (fn [col]
                                     (let [cn (:column-name col)
                                           ct (or (:source-column-type col) (:column-type col))]
                                       (if (geometry-type? ct)
                                         (str "ST_AsText(`" cn "`) AS `" cn "`")
                                         (str "`" cn "`"))))
                                   columns))
                   "*")]
    (str "SELECT " col-list " FROM `" mysql-table "`")))

(defn- stream-ranges!
  "Execute sqls sequentially on conn, closing each RS/stmt before the next.
   Returns a lazy seq of all rows across all range queries."
  [^Connection conn active-stmt active-rs sqls columns]
  (when (seq sqls)
    (let [sql      (first sqls)
          rest-sql (rest sqls)]
      (try (when-let [rs @active-rs]   (.close ^ResultSet rs))   (catch Exception _))
      (try (when-let [st @active-stmt] (.close ^PreparedStatement st)) (catch Exception _))
      (vreset! active-rs nil)
      (vreset! active-stmt nil)
      (let [stmt (.prepareStatement ^Connection conn sql)
            _ (doto ^PreparedStatement stmt
                (.setFetchSize Integer/MIN_VALUE)
                (.setFetchDirection ResultSet/FETCH_FORWARD))
            _ (vreset! active-stmt stmt)
            rs (.executeQuery ^PreparedStatement stmt)
            _ (vreset! active-rs rs)
            meta (.getMetaData rs)
            n (.getColumnCount meta)
            jtypes (vec (map #(.getColumnTypeName meta %) (range 1 (inc n))))]
        (letfn [(next-row []
                  (let [has-next (try (.next rs)
                                      (catch Exception e
                                        (try (.close rs) (catch Exception _))
                                        (try (.close stmt) (catch Exception _))
                                        (vreset! active-rs nil)
                                        (vreset! active-stmt nil)
                                        (throw e)))]
                    (if has-next
                      (lazy-seq
                       (cons (loop [i 1 result (transient [])]
                               (if (<= i n)
                                 (recur (inc i)
                                        (conj! result
                                               (convert-mysql-value
                                                (.getObject rs i)
                                                (nth jtypes (dec i))
                                                (nth columns (dec i) nil)
                                                rs i)))
                                 (persistent! result)))
                             (next-row)))
                      ;; RS exhausted — open next range
                      (stream-ranges! conn active-stmt active-rs rest-sql columns))))]
          (next-row))))))

(deftype MySQLSource
         [^Connection conn
          ^String database
          ^String schema-name
          table-spec
          decoding-rules  ; seq of {:patterns [...] :encoding charset} or nil
          server-variant  ; {:variant :mysql/:mariadb :major-version N :version-string "..."}
          active-stmt     ; volatile! — current streaming PreparedStatement, or nil
          active-rs       ; volatile! — current streaming ResultSet, or nil
          uri-map         ; connection parameters map, used by partition-source to clone connections
          range-col       ; String pk column for range queries, or nil (no partitioning)
          ranges]         ; seq of [lo hi] Long pairs (one per partition range), or nil

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
      (let [;; expression column in information_schema.statistics is MySQL 8.0+ only;
            ;; MariaDB and MySQL 5.x must use the simpler query without it.
            use-simple-index-query? (or (= :mariadb (:variant server-variant))
                                        (< (:major-version server-variant) 8))]
        (->> ts
             (map (fn [t]
                    (let [table-name (apply-identifier-case (:table_name t) id-case)
                          cols (columns conn
                                        {:schema schema-name
                                         :table (:table_name t)})
                          pkeys (table-pkeys conn
                                             {:schema schema-name
                                              :table (:table_name t)})
                          idxes (if use-simple-index-query?
                                  (table-indexes-mariadb conn {:schema schema-name
                                                               :table (:table_name t)})
                                  (table-indexes conn {:schema schema-name
                                                       :table (:table_name t)}))
                          fks   (table-fkeys conn
                                             {:schema schema-name
                                              :table (:table_name t)})
                          ;; CHECK constraints: MySQL 8.0.16+; empty on older versions
                          checks (try
                                   (table-checks conn {:schema schema-name
                                                       :table (:table_name t)})
                                   (catch Exception _
                                     []))]
                      {:table-name        table-name
                       :source-table-name (:table_name t)
                       :schema            schema-name
                       :table-comment     (not-empty (:table_comment t))
                       :columns    (mapv (fn [c]
                                           (let [gen-expr (mysql-gen-expr->pg
                                                           (:generation_expression c))]
                                             (cond-> {:column-name      (apply-identifier-case (:column_name c) id-case)
                                                      :column-type      (:column_type c)
                                                      :is-nullable      (= "YES" (:is_nullable c))
                                                      :column-default   (:column_default c)
                                                      :extra            (:extra c)
                                                      :column-key       (:column_key c)
                                                      :ordinal-position (:ordinal_position c)
                                                      :column-comment   (:column_comment c)}
                                               gen-expr (assoc :generated-expression gen-expr))))
                                         cols)
                       :primary-key (mapv #(apply-identifier-case % id-case) (mapv :column_name pkeys))
                       :indexes    (mapv (fn [idx]
                                           {:name       (:index_name idx)
                                            :unique     (zero? (:non_unique idx))
                                            :index-type (:index_type idx)
                                            :columns    (mapv (fn [col]
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
                                         fks)
                       :checks     (mapv (fn [ck]
                                           {:constraint-name (:constraint_name ck)
                                            :check-clause    (:check_clause ck)})
                                         checks)})))
             (filter (fn [t]
                       (if-let [filter-fn (:table-filter table-spec)]
                         (filter-fn (:table-name t))
                         true)))
             (sort-by :table-name)))))

  (read-rows [_ table-spec-entry]
    (let [{:keys [table-name source-table-name columns citus-read-sql]} table-spec-entry
          mysql-table (or source-table-name table-name)]
      (when-let [charset (decoding-as-charset mysql-table decoding-rules)]
        (try
          (jdbc/execute! conn [(str "SET NAMES '" charset "'")])
          (log/info (str "SET NAMES '" charset "' for table " mysql-table))
          (catch Exception e
            (log/warn (str "SET NAMES failed for " mysql-table ": " (.getMessage e))))))
      (let [base-sql (or citus-read-sql (mysql-select-sql columns mysql-table))
            sqls     (if (seq ranges)
                       (mapv (fn [[lo hi]]
                               (str base-sql
                                    " WHERE `" range-col "` >= " lo
                                    " AND `" range-col "` < " hi))
                             ranges)
                       [base-sql])]
        (stream-ranges! conn active-stmt active-rs sqls columns))))
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

  (partition-source [this table-spec-entry n chunk-bytes]
    (mysql-partition-source this table-spec-entry n chunk-bytes))

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
  "Create a MySQL JDBC connection.
   Uses :jdbc-url when present so all query parameters (useSSL, sslmode, etc.)
   reach the driver unchanged. Falls back to constructing a URL from parts."
  [uri-map]
  (let [host (or (:host uri-map) "localhost")
        port (or (:port uri-map) 3306)
        db   (or (:db uri-map) "")
        url  (or (:jdbc-url uri-map)
                 (str "jdbc:mysql://" host ":" port "/" db))
        props (java.util.Properties.)]
    (when (:user uri-map) (.setProperty props "user" (:user uri-map)))
    (when (:password uri-map) (.setProperty props "password" (:password uri-map)))
    (.setProperty props "characterEncoding" "UTF-8")
    (.setProperty props "rewriteBatchedStatements" "true")
    (.setProperty props "useCursorFetch" "false")
    (.setProperty props "zeroDateTimeBehavior" "CONVERT_TO_NULL")
    (DriverManager/getConnection url props)))

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
    (->MySQLSource conn (:db uri-map) (:db uri-map) ts decoding-as-rules sv (volatile! nil) (volatile! nil) uri-map nil nil)))

(defn mysql-partition-source
  [^MySQLSource src table-spec-entry n chunk-bytes]
  (let [conn        (.-conn src)
        database    (.-database src)
        schema-name (.-schema_name src)
        table-spec  (.-table_spec src)
        decoding-rules (.-decoding_rules src)
        server-variant (.-server_variant src)
        uri-map     (.-uri_map src)]
    (when (and uri-map (pos? n) (pos? chunk-bytes))
      (let [mysql-table (or (:source-table-name table-spec-entry) (:table-name table-spec-entry))
            pkeys       (:primary-key table-spec-entry)
            pk-col      (when (= 1 (count pkeys)) (first pkeys))
            pk-int?     (when pk-col
                          (let [col-info (first (filter #(= pk-col (:column-name %)) (:columns table-spec-entry)))]
                            (some #(str/starts-with? (str/lower-case (or (:column-type col-info) "")) %)
                                  ["int" "bigint" "smallint" "tinyint" "mediumint"])))]
        (when pk-int?
          (try
            (let [avg-row (or (:avg_row_length
                               (table-avg-row-length conn {:schema schema-name :table mysql-table}))
                              0)
                  rows-per-chunk (max 1000 (long (/ chunk-bytes (max 1 avg-row))))
                  pk-range  (table-pk-min-max conn {:pk-col (str "`" pk-col "`")
                                                    :table  (str "`" mysql-table "`")})
                  min-row   (:lo pk-range)
                  max-row   (:hi pk-range)]
              (when (and min-row max-row)
                (let [lo   (long min-row)
                      hi   (inc (long max-row))
                      rngs (vec (split-range lo hi rows-per-chunk))]
                  (when (>= (count rngs) 2)
                    (let [buckets (distribute rngs n)]
                      (keep-indexed
                       (fn [_i bucket]
                         (when (seq bucket)
                           (->MySQLSource
                            (connection uri-map)
                            database schema-name table-spec
                            decoding-rules server-variant
                            (volatile! nil) (volatile! nil)
                            uri-map pk-col (vec bucket))))
                       buckets))))))
            (catch Exception e
              (log/warn (str "partition-source: could not partition " mysql-table ": " (.getMessage e)))
              nil)))))))

(ns pgloader.load-file.ast
  (:require [instaparse.core :as insta]
            [clojure.string :as str]
            [pgloader.pg-service :as pg-service])
  (:import [java.net URI]))

(set! *warn-on-reflection* true)

(defrecord LoadCommand
           [load-type
            source
            target
            with-options
            set-parameters
            cast-rules
            filters
            alter-schema
            alter-table
            before-load
            after-load
            materialize-views
            distribute-rules
            decoding-as
   ;; For :archive load-type: ordered list of sub-LoadCommand records.
            commands])

(defn parse-uri
  "Parse a connection URI string into a structured map.
   Accepts both pgloader-native schemes (postgresql://, mysql://, mssql://, …)
   and JDBC URLs (jdbc:postgresql://, jdbc:mysql://, jdbc:sqlserver://, …).

   Every returned map includes :jdbc-url — the exact URL to hand to
   DriverManager/getConnection. Connection functions use :jdbc-url directly
   so all driver-specific query parameters (useSSL, sslmode, trustServerCertificate,
   connectTimeout, …) are passed through to the driver unchanged.

   For pg_service.conf lookup use postgresql:///?service=<name>."
  [^String uri-str]
  (cond
    ;; ── JDBC URL: pass through to driver unchanged ──
    ;; The MSSQL JDBC URL uses semicolons (jdbc:sqlserver://host;param=val;...)
    ;; which java.net.URI can't parse. Hand it straight to the driver.
    (str/starts-with? uri-str "jdbc:sqlserver:")
    {:type :mssql :raw uri-str :jdbc-url uri-str}

    (str/starts-with? uri-str "jdbc:sqlite:")
    {:type     :sqlite
     :path     (subs uri-str (count "jdbc:sqlite:"))
     :raw      uri-str
     :jdbc-url uri-str}

    (str/starts-with? uri-str "jdbc:")
    ;; jdbc:postgresql://, jdbc:mysql://, jdbc:mariadb:// — strip "jdbc:" and
    ;; parse the inner URI for host/port/db/user/password metadata; keep :jdbc-url
    ;; so the connection function uses the original URL (preserving all query params).
    (let [inner    (subs uri-str 5)          ; strip "jdbc:"
          inner-map (parse-uri inner)]       ; recurse without prefix
      (assoc inner-map :raw uri-str :jdbc-url uri-str))

    ;; ── Standard pgloader URI ──
    :else
    (let [;; java.net.URI rejects spaces and a handful of other characters.
          ;; Percent-encode them so URI. doesn't throw; getPath/getSchemeSpecificPart
          ;; decode them back, giving us the original string in the result map.
          safe-str  (str/replace uri-str " " "%20")
          uri       (URI. safe-str)
          scheme    (.getScheme uri)
          path      (.getPath uri)
          user-info (.getUserInfo uri)
          [user password] (when user-info
                            (let [parts (str/split user-info #":" 2)]
                              [(first parts) (when (> (count parts) 1) (second parts))]))
          raw-query (.getRawQuery uri)   ; preserve original query string (un-decoded)
          query     (.getQuery uri)
          service-name (when query
                         (some->> (str/split query #"&")
                                  (some #(when (str/starts-with? % "service=")
                                           (subs % (count "service="))))))
          table-from-query (when (and query (not (re-find #"=" query)))
                             (let [q (java.net.URLDecoder/decode query "UTF-8")]
                               (if (re-find #"\." q)
                                 (let [[s t] (str/split q #"\." 2)]
                                   {:schema s :table t})
                                 {:table q})))
          db        (when path (str/replace path #"^/" ""))
          host      (or (.getHost uri) "localhost")]
      (case scheme
        ("postgresql" "pgsql")
        (if service-name
          (let [svc (pgloader.pg-service/lookup service-name)]
            (when-not svc
              (throw (ex-info (str "pg_service.conf: service '" service-name "' not found")
                              {:service service-name})))
            (merge {:type :pgsql :raw uri-str} svc))
          (pgloader.pg-service/apply-pgpass
           (merge {:type     :pgsql
                   :host     host
                   :port     (if (pos? (.getPort uri)) (.getPort uri) 5432)
                   :db       db
                   :user     user
                   :password password
                   :raw      uri-str
                   :jdbc-url (str "jdbc:postgresql://" host
                                  ":" (if (pos? (.getPort uri)) (.getPort uri) 5432)
                                  "/" db
                                  (when raw-query (str "?" raw-query)))}
                  table-from-query)))

        ("mssql" "sqlserver")
        (merge {:type     :mssql
                :host     host
                :port     (if (pos? (.getPort uri)) (.getPort uri) 1433)
                :db       db
                :user     user
                :password password
                :raw      uri-str
                :jdbc-url (str "jdbc:sqlserver://" host
                               ":" (if (pos? (.getPort uri)) (.getPort uri) 1433)
                               ";databaseName=" db ";encrypt=false"
                               (when raw-query (str ";" (str/replace raw-query #"&" ";"))))}
               table-from-query)

        ("mysql" "mariadb")
        {:type     (keyword scheme)
         :host     host
         :port     (if (pos? (.getPort uri)) (.getPort uri) 3306)
         :db       db
         :user     user
         :password password
         :raw      uri-str
         :jdbc-url (str "jdbc:mysql://" host
                        ":" (if (pos? (.getPort uri)) (.getPort uri) 3306)
                        "/" db
                        (when raw-query (str "?" raw-query)))}

        "csv"
        {:type :csv
         :path (str/replace (.getSchemeSpecificPart uri) #"^//" "")
         :raw  uri-str}

        "sqlite"
        {:type     :sqlite
         :path     (str/replace (.getSchemeSpecificPart uri) #"^//" "")
         :raw      uri-str
         :jdbc-url (str "jdbc:sqlite:" (str/replace (.getSchemeSpecificPart uri) #"^//" ""))}

        {:type (keyword scheme)
         :raw  uri-str}))))

(defn- parse-size
  "Parse size strings like '128MB', '20GB', '500KB' or plain integers"
  [s]
  (when s
    (try
      (Integer/parseInt s)
      (catch NumberFormatException _
        (if-let [[_ n unit] (re-find #"(\d+)\s*(MB|GB|KB)" (clojure.string/upper-case s))]
          (let [base (Integer/parseInt n)]
            (case unit
              "MB" (* base 1024 1024)
              "GB" (* base 1024 1024 1024)
              "KB" (* base 1024)
              base))
          (parse-size (re-find #"\d+" s)))))))

(defn- parse-cast-options
  "Parse cast options from a hiccup tree node"
  [opts]
  (reduce (fn [acc opt]
            (case opt
              :drop-not-null (assoc acc :drop-not-null true)
              :drop-default (assoc acc :drop-default true)
              :drop-extra (assoc acc :drop-extra true)
              :set-not-null (assoc acc :set-not-null true)
              :keep-not-null (assoc acc :drop-not-null false)
              :drop-typemod (assoc acc :drop-typemod true)
              :keep-typemod (assoc acc :drop-typemod false)
              acc))
          {} opts))

(defn- hiccup->decoding-as
  "Parse a decoding-as-clause node into {:patterns [...] :encoding charset}."
  [node]
  (when (and (vector? node) (= :decoding-as-clause (first node)))
    (let [children (rest node)
          charset (let [last-c (last children)]
                    (if (string? last-c)
                      last-c
                      (when (vector? last-c) (second last-c))))
          pat-list (first (filter #(= :decoding-pattern-list (first %)) children))
          patterns (when pat-list
                     (mapv (fn [p]
                             (let [v (second p)]
                               (if (string? v)
                                 {:type :regex :value (str/replace v #"^/(.*)/$" "$1")}
                                 {:type :exact :value (second v)})))
                           (filter #(= :table-name-pattern (first %)) (rest pat-list))))]
      {:patterns patterns :encoding charset})))

(defn- hiccup->distribute-rule
  "Convert a single distribute-clause hiccup node into a rule map."
  [node]
  (when (vector? node)
    (let [inner  (if (= :distribute-clause (first node)) (second node) node)
          inner-children (rest inner)]
      (case (first inner)
        :distribute-reference
        (let [tbl (second (first (filter #(= :table-name (first %)) inner-children)))]
          {:type :reference :table tbl})
        :distribute-using
        (let [tbl (second (first (filter #(= :table-name (first %)) inner-children)))
              col (second (first (filter #(= :column-name (first %)) inner-children)))]
          {:type :distributed :table tbl :using col})
        :distribute-using-from
        (let [tbl (second (first (filter #(= :table-name (first %)) inner-children)))
              col (second (first (filter #(= :column-name (first %)) inner-children)))
              ft  (first (filter #(= :from-table-list (first %)) inner-children))]
          {:type :distributed-from
           :table tbl
           :using col
           :from (vec (map second (filter #(= :table-name (first %)) (rest ft))))})
        nil))))

(defn- hiccup->distribute-rules
  "Convert a distribute-section hiccup node into a vector of rule maps."
  [node]
  (when (and (vector? node) (= :distribute-section (first node)))
    (into [] (keep hiccup->distribute-rule (rest node)))))

(defn- ^{:no-doc true} reconstruct-when-expr
  "Reconstruct the text of a when-expr node, handling nested parentheses.
   Nested when-expr nodes appear as children of :when-inner wrappers."
  [node]
  (apply str (map (fn [c]
                    (cond
                      (= :when-expr (first c))
                      (str "(" (reconstruct-when-expr c) ")")
                      (and (= :when-inner (first c))
                           (= 2 (count c))
                           (vector? (second c))
                           (= :when-expr (first (second c))))
                      (str "(" (reconstruct-when-expr (second c)) ")")
                      :else
                      (second c)))
                  (rest node))))

(defn- hiccup->cast-rule
  "Convert a hiccup cast-rule node into a cast rule map."
  [node]
  (when (and (vector? node) (= :cast-rule (first node)))
    (let [children (rest node)
          type-cast (some #(= :type-cast (first %)) children)
          column-cast (some #(= :column-cast (first %)) children)
          inner (first (filter #(or (= :type-cast (first %))
                                    (= :column-cast (first %)))
                               children))
          inner-children (vec (rest inner))
          source (when type-cast
                   (let [source-node (first (filter #(= :cast-type-name (first %)) inner-children))]
                     {:type :type
                      :name (clojure.string/join " " (map second (filter #(= :word-part (first %)) (rest source-node))))}))
          source (if column-cast
                   (let [col-ref (first (filter #(= :column-ref (first %)) inner-children))
                         parts (rest col-ref)]
                     (when col-ref
                       (let [tbl (second (first (filter #(= :table-name (first %)) parts)))
                             col (second (first (filter #(= :column-name (first %)) parts)))]
                         {:type :column :table tbl :column col})))
                   source)
          ;; Unwrap [:cast-option [...]] wrappers so option tag nodes are at top level
          flat-cast-opts (mapcat (fn [n]
                                   (if (and (vector? n) (= :cast-option (first n)))
                                     (rest n)
                                     [n]))
                                 inner-children)
          target-node (first (filter #(= :target-type-name (first %)) inner-children))
          target (when target-node
                   (let [target-inner (second target-node)]
                     (if (= :dq-string (first target-inner))
                       (second target-inner)
                       (clojure.string/join " " (map second (filter #(= :word-part (first %)) (rest target-inner)))))))
          using-fn (some (fn [n]
                           (when (and (vector? n) (= :using-fn (first n)))
                             (keyword (second (some #(when (and (vector? %) (= :fn-name (first %))) %)
                                                    (rest n))))))
                         flat-cast-opts)
          when-node (first (filter #(= :when-or-unsigned (first %)) inner-children))
          when-cond (when when-node
                      (let [wc (vec (rest when-node))]
                        (cond
                          (empty? wc) {:when-unsigned true}
                          (some #(= :when-default-val (first %)) wc)
                          (let [dv (first (filter #(= :when-default-val (first %)) wc))
                                vi (second dv)]
                            {:when-default (if (= :dq-string (first vi))
                                             (second vi)
                                             (clojure.string/join " " (map second (filter #(= :word-part (first %)) (rest vi)))))})
                          (some #(= :when-expr (first %)) wc)
                          (let [we (first (filter #(= :when-expr (first %)) wc))
                                parts (map (fn [c]
                                             (cond
                                               (= :when-expr (first c))
                                               (str "(" (reconstruct-when-expr c) ")")
                                               (and (= :when-inner (first c))
                                                    (= 2 (count c))
                                                    (vector? (second c))
                                                    (= :when-expr (first (second c))))
                                               (str "(" (reconstruct-when-expr (second c)) ")")
                                               :else
                                               (second c)))
                                           (rest we))]
                            {:when-extra (clojure.string/join "" parts)}))))
          drop-opts (parse-cast-options (map first flat-cast-opts))]
      (cond-> {:target-type target}
        source (assoc :source source)
        (seq drop-opts) (assoc :options drop-opts)
        using-fn (assoc :using using-fn)
        when-cond (merge when-cond)))))

(defn- hiccup->option-keyword
  "Convert a hiccup option node to a keyword or [keyword value] pair.
   Options are wrapped in [:db-option [:keyword ...]] or [:db-option [:batch-rows [:integer N]]]."
  [node]
  (when (and (vector? node) (= :db-option (first node)))
    (let [inner (second node)]
      (when (vector? inner)
        (let [tag (first inner)]
          (case tag
            :batch-rows          [tag (Integer/parseInt (second (second inner)))]
            :batch-size          [tag (second (second inner))]
            :prefetch-rows       [tag (Integer/parseInt (second (second inner)))]
            :rows-per-range      [tag (Integer/parseInt (second (second inner)))]
            :workers             [tag (Integer/parseInt (second (second inner)))]
            :concurrency         [tag (Integer/parseInt (second (second inner)))]
            :max-parallel-create-index [tag (Integer/parseInt (second (second inner)))]
            :chunk-size          [tag (let [ds (second inner)
                                            n  (Long/parseLong (second (second ds)))
                                            u  (if (> (count ds) 2) (second (nth ds 2)) "B")]
                                        (* n (case u "KB" 1024 "MB" (* 1024 1024) "GB" (* 1024 1024 1024) 1)))]
            :multiple-readers    [:multiple-readers true]
            :single-reader       [:multiple-readers false]
            :identifier-case     (first (second inner))
            :no-reset-sequences  [:reset-sequences false]
            :drop-schema         [:drop-schema true]
            :reindex             [:reindex true]
            :preserve-index-names [:preserve-index-names true]
            :uniquify-index-names [:uniquify-index-names true]
            tag))))))

(defn- parse-column-item
  "Parse a column-item node. Returns {:name string :date-format string-or-nil :nullif string-or-nil}."
  [node]
  (when (and (vector? node) (= :column-item (first node)))
    (let [children (rest node)
          col-name (second (first (filter #(= :column-name (first %)) children)))
          df-node (some #(when (= :date-format-spec (first %)) %) children)
          nf-node (some #(when (= :null-if-spec (first %)) %) children)
          date-format (when df-node
                        (second (second df-node)))
          nullif (when nf-node
                   (second (second nf-node)))]
      {:name col-name :date-format date-format :nullif nullif})))

(defn- hiccup->set-option
  [node]
  (when (and (vector? node) (or (= :set-option (first node))
                                (= :set-mysql-option (first node))))
    (let [is-mysql  (= :set-mysql-option (first node))
          children  (rest node)
          var-tag   (if is-mysql :mysql-var :pg-var)
          var-name  (second (first (filter #(and (vector? %) (= var-tag (first %))) children)))
          val       (second (first (filter #(and (vector? %) (= :quoted-string (first %))) children)))]
      {:var var-name :value val :is-mysql is-mysql})))

(defn- interpret-escape
  "Interpret common escape sequences in a single-character string.
   Supports hex byte literals like 0x02, and named escapes."
  [s]
  (if (re-find #"(?i)^0x[0-9a-f]{2}$" s)
    (char (Integer/parseInt (subs s 2) 16))
    (case s
      "\\t" \tab
      "\\n" \newline
      "\\r" \return
      "\\0" \0
      (first s))))

(defn- hiccup->csv-option
  [node]
  (when (vector? node)
    (let [tag (first node)]
      (case tag
        :csv-option (hiccup->csv-option (second node))
        :skip-header {:skip-header (Integer/parseInt (second (second node)))}
        :csv-encoding {:encoding (second (second node))}
        :fields-terminated {:delimiter (interpret-escape (second (second node)))}
        :fields-enclosed {:quote-char (interpret-escape (second (second node)))}
        :fields-escaped (let [child (second node)
                              escape-type (first child)
                              escape-child (when (= :escaped-quote escape-type) (second child))]
                          (case (first escape-child)
                            :quoted-char {:escape-char (interpret-escape (second escape-child))}
                            :backslash-escape {:escape-char \\}
                            :double-escape {:escape-char \"}
                            {:escape-char \\}))
        :fields-not-enclosed {:quote-char nil}
        :create-tables {:create-tables true}
        :create-no-tables {:create-tables false}
        :nullif {:nullif (second (second node))}
        :keep-unquoted-blanks {:keep-unquoted-blanks true}
        :trim-unquoted-blanks {:trim-unquoted-blanks true}
        :truncate {:truncate true}
        :disable-triggers {:disable-triggers true}
        :batch-rows {:batch-rows (Integer/parseInt (second (second node)))}
        :batch-size {:batch-size (second (second node))}
        :batch-concurrency {:batch-concurrency (Integer/parseInt (second (second node)))}
        :prefetch-rows {:prefetch-rows (Integer/parseInt (second (second node)))}
        :csv-header {:csv-header true}
        :csv-escape-mode {:escape-mode :following}
        :lines-terminated {:lines-terminated (interpret-escape (second (second node)))}
        :drop-indexes {:drop-indexes true}
        nil))))

(defn- hiccup->copy-option
  [node]
  (when (vector? node)
    (let [tag (first node)]
      (case tag
        :copy-option (hiccup->copy-option (second node))
        :option-delimiter {:delimiter (interpret-escape (second (second node)))}
        :create-table {:create-tables true}
        :option-null {:nullif (second (second node))}
        :truncate {:truncate true}
        :create-tables {:create-tables true}
        :create-no-tables {:create-tables false}
        :batch-rows {:batch-rows (Integer/parseInt (second (second node)))}
        :batch-size {:batch-size (second (second node))}
        :batch-concurrency {:batch-concurrency (Integer/parseInt (second (second node)))}
        :disable-triggers {:disable-triggers true}
        :drop-indexes {:drop-indexes true}
        nil))))

(defn- hiccup->dbf-option
  [node]
  (when (vector? node)
    (let [tag (first node)]
      (case tag
        :dbf-option (hiccup->dbf-option (second node))
        :dbf-encoding {:encoding (second (second node))}
        :create-table {:create-tables true}
        :truncate {:truncate true}
        :create-tables {:create-tables true}
        :create-no-tables {:create-tables false}
        :batch-rows {:batch-rows (Integer/parseInt (second (second node)))}
        :batch-size {:batch-size (second (second node))}
        :batch-concurrency {:batch-concurrency (Integer/parseInt (second (second node)))}
        :disable-triggers {:disable-triggers true}
        :drop-indexes {:drop-indexes true}
        nil))))

(defn- s-expr-content
  "Reconstruct an S-expression string from the s-expr parse tree.
   Grammar: s-expr = <'('> s-expr-inner* <')'>
            s-expr-inner = s-expr | #'[^()]+'.
   Children of an :s-expr node are :s-expr-inner nodes; each :s-expr-inner
   holds either a plain string or a nested :s-expr."
  [node]
  (apply str (map (fn [c]
                    (when (vector? c)
                      (case (first c)
                        :s-expr
                        (str "(" (s-expr-content c) ")")
                        :s-expr-inner
                        (let [inner (second c)]
                          (if (vector? inner)
                            (str "(" (s-expr-content inner) ")")
                            (str inner)))
                        "")))
                  (rest node))))

(defn- parse-target-column-def
  "Parse a target-column-def node into a projection map.
   {:column-name name :target-type type :using expr}"
  [node]
  (when (and (vector? node) (= :target-column-def (first node)))
    (let [children (rest node)
          col-name (second (first children))
          type-node (first (filter #(= :target-type-name (first %)) children))
          using-node (first (filter #(= :using-expr (first %)) children))
          target-type (when type-node
                        (let [inner (second type-node)]
                          (if (= :dq-string (first inner))
                            (second inner)
                            (clojure.string/join " " (map second (filter #(= :word-part (first %)) (rest inner)))))))
          using-expr (when using-node
                       (let [inner (second using-node)]
                         (case (first inner)
                           :quoted-string   (second inner)
                           :using-dq-string (second inner)
                           :reader-fn       (str "#(" (s-expr-content (second inner)) ")")
                           (str "(" (s-expr-content inner) ")"))))]
      {:projection {:column-name col-name
                    :target-type target-type
                    :using using-expr}})))

(defn transform
  "Transform an instaparse hiccup tree into a LoadCommand record.
   The tree comes from the instaparse parser with :string-ci true,
   so tags are lowercase keywords."
  [tree & [inline-data]]
  (when (vector? tree)
    (let [tag (first tree)]
      (case tag
        :load-command
        (let [children    (rest tree)
              csv-node    (first (filter #(= :load-csv      (first %)) children))
              copy-node   (first (filter #(= :load-copy     (first %)) children))
              dbf-node    (first (filter #(= :load-dbf      (first %)) children))
              db-node     (first (filter #(= :load-database (first %)) children))
              fixed-node  (first (filter #(= :load-fixed    (first %)) children))
              archive-node (first (filter #(= :load-archive  (first %)) children))]
          (cond csv-node     (transform csv-node inline-data)
                copy-node    (transform copy-node inline-data)
                dbf-node     (transform dbf-node inline-data)
                db-node      (transform db-node inline-data)
                fixed-node   (transform fixed-node inline-data)
                archive-node (transform archive-node inline-data)))

        :load-csv
        (let [children (mapcat (fn [c] (if (= :load-csv-clause (first c)) (rest c) [c])) (rest tree))
              from-node (first (filter #(= :from-source (first %)) children))
              source-enc (some #(when (= :source-encoding (first %))
                                  (let [v (second %)]
                                    (if (string? v) v (second v))))
                               children)
              from-source (when from-node
                            (let [inner (second from-node)]
                              (case (first inner)
                                :filepath {:type :csv :path (second inner) :encoding source-enc}
                                :stdin-source {:type :csv :stdin true :encoding source-enc}
                                :inline-source {:type :csv :inline true :encoding source-enc}
                                :glob-source
                                (let [gchildren (rest inner)
                                      pattern (second (first (filter #(= :file-pattern (first %)) gchildren)))
                                      dir-path (second (first (filter #(= :filepath (first %)) gchildren)))]
                                  {:type :csv :glob-pattern pattern :directory dir-path :encoding source-enc})
                                :copy-source {:type :copy :path (second inner) :encoding source-enc}
                                :dbf-source {:type :dbf :path (second inner) :encoding source-enc}
                                {:type :csv :path nil})))
              pg-uris  (filter #(= :pg-uri (first %)) children)
              pg-uri   (parse-uri (second (first pg-uris)))
              qualified (first (filter #(= :qualified-name (first %)) children))
              with-clause (first (filter #(= :with-csv-clause (first %)) children))
              pg-uri-pos (first (keep-indexed #(when (= :pg-uri (first %2)) %1) children))
              source-col-list (when pg-uri-pos
                                (some #(when (= :column-list (first %)) %)
                                      (take pg-uri-pos children)))
              target-col-lists (when pg-uri-pos
                                 (filter #(= :column-list (first %))
                                         (drop (inc pg-uri-pos) children)))
              csv-options (when with-clause
                            (let [opts (rest with-clause)]
                              (reduce merge {}
                                      (map hiccup->csv-option
                                           (filter vector? opts)))))
              set-params (mapcat (fn [c]
                                   (when (and (vector? c) (= :set-clause (first c)))
                                     (keep hiccup->set-option (rest c))))
                                 children)
              before-load-node (first (filter #(= :before-load-do (first %)) children))
              before-load (when before-load-node
                            (let [commands (rest (second before-load-node))]
                              (mapv #(second %) (filter vector? commands))))]
          (let [target-table (when qualified
                               (let [parts (vec (rest qualified))]
                                 (second (if (= 2 (count parts))
                                           (nth parts 1)
                                           (nth parts 0)))))
                target-schema (when (and qualified
                                         (= 2 (count (vec (rest qualified)))))
                                (-> qualified rest vec first second))
                target-table-clause (first (filter #(= :target-table-clause (first %)) children))
                tt-table (when target-table-clause
                           (let [parts (vec (rest target-table-clause))
                                 table-ref-node (first (filter #(= :table-ref (first %)) parts))
                                 col-list (first (filter #(= :target-column-def-list (first %)) parts))
                                 table-info (when table-ref-node
                                              (let [inner (second table-ref-node)]
                                                (if (= :qualified-name (first inner))
                                                  (let [qparts (vec (rest inner))]
                                                    (if (= 2 (count qparts))
                                                      {:schema (second (first qparts)) :table (second (second qparts))}
                                                      {:table (second (first qparts))}))
                                                  {:table (second inner)})))
                                 proj (when col-list
                                        {:projections (vec (keep :projection (map parse-target-column-def (rest col-list))))})]
                             (merge table-info proj)))
                target-columns-clause (first (filter #(= :target-columns-clause (first %)) children))
                tt-columns (when target-columns-clause
                             (mapv #(second %) (rest (first (filter #(= :column-list (first %)) (rest target-columns-clause))))))
                tt-projections (when target-columns-clause
                                 (vec (keep :projection (map parse-target-column-def (rest (first (filter #(= :target-column-def-list (first %)) (rest target-columns-clause))))))))
                having-node (first (filter #(= :having-fields (first %)) children))
                having-columns (when having-node
                                 (mapv :name (map parse-column-item
                                                  (filter #(= :column-item (first %))
                                                          (rest (first (filter #(= :column-list (first %)) (rest having-node))))))))
                after-load-node (first (filter #(= :after-load-do (first %)) children))
                after-load (when after-load-node
                             (let [commands (rest (second after-load-node))]
                               (mapv #(second %) (filter vector? commands))))
                cols (when source-col-list
                       (mapv :name (map parse-column-item
                                        (filter #(= :column-item (first %))
                                                (rest source-col-list)))))
                col-formats (when source-col-list
                              (seq (filter :date-format
                                           (map parse-column-item
                                                (filter #(= :column-item (first %))
                                                        (rest source-col-list))))))
                col-nullifs (when source-col-list
                              (seq (filter :nullif
                                           (map parse-column-item
                                                (filter #(= :column-item (first %))
                                                        (rest source-col-list))))))
                bare-target-cols (when (seq target-col-lists)
                                   (mapv :name (map parse-column-item
                                                    (filter #(= :column-item (first %))
                                                            (rest (first target-col-lists))))))]
            (->LoadCommand
             :csv
             (merge from-source
                    {:columns (or having-columns cols)}
                    (when (and inline-data (:inline from-source))
                      {:inline-data inline-data})
                    (when col-formats
                      {:column-formats col-formats})
                    (when col-nullifs
                      {:column-nullifs col-nullifs}))
             {:type :pgsql :target-uri pg-uri
              :schema (or (:schema tt-table) target-schema)
              :table (or (:table tt-table) target-table)}
             (merge {:skip-header 0
                     :quote-char \"}
                    (when (and target-schema target-table)
                      {:target-schema target-schema
                       :target-table target-table})
                    (when (and (nil? target-table) (:table pg-uri))
                      {:target-schema (or (:schema pg-uri) "public")
                       :target-table (:table pg-uri)})
                    (when tt-table
                      (merge {:target-schema (:schema tt-table)
                              :target-table (:table tt-table)}
                             (when (:projections tt-table)
                               {:projections (:projections tt-table)})))
                    (when (seq bare-target-cols)
                      {:target-columns bare-target-cols})
                    (when (seq tt-columns)
                      {:target-columns tt-columns})
                    (when (seq tt-projections)
                      {:projections tt-projections})
                    csv-options)
             (seq set-params) nil nil nil nil (seq before-load) (seq after-load) nil nil nil nil)))

        :load-copy
        (let [children (mapcat (fn [c] (if (= :load-copy-clause (first c)) (rest c) [c])) (rest tree))
              copy-node (first (filter #(= :copy-source (first %)) children))
              source-enc (some #(when (= :source-encoding (first %))
                                  (let [v (second %)]
                                    (if (string? v) v (second v))))
                               children)
              source (when copy-node
                       {:type :copy :path (second copy-node) :encoding source-enc})
              pg-uris  (filter #(= :pg-uri (first %)) children)
              pg-uri   (parse-uri (second (first pg-uris)))
              qualified (first (filter #(= :qualified-name (first %)) children))
              with-clause (first (filter #(= :with-copy-clause (first %)) children))
              pg-uri-pos (first (keep-indexed #(when (= :pg-uri (first %2)) %1) children))
              source-col-list (when pg-uri-pos
                                (some #(when (= :column-list (first %)) %)
                                      (take pg-uri-pos children)))
              copy-options (when with-clause
                             (let [opts (rest with-clause)]
                               (reduce merge {}
                                       (map hiccup->copy-option
                                            (filter vector? opts)))))
              set-params (mapcat (fn [c]
                                   (when (and (vector? c) (= :set-clause (first c)))
                                     (keep hiccup->set-option (rest c))))
                                 children)
              before-load-node (first (filter #(= :before-load-do (first %)) children))
              before-load (when before-load-node
                            (let [commands (rest (second before-load-node))]
                              (mapv #(second %) (filter vector? commands))))
              target-table (when qualified
                             (let [parts (vec (rest qualified))]
                               (second (if (= 2 (count parts))
                                         (nth parts 1)
                                         (nth parts 0)))))
              target-schema (when (and qualified
                                       (= 2 (count (vec (rest qualified)))))
                              (-> qualified rest vec first second))
              target-table-clause (first (filter #(= :target-table-clause (first %)) children))
              tt-table (when target-table-clause
                         (let [parts (vec (rest target-table-clause))
                               table-ref-node (first (filter #(= :table-ref (first %)) parts))
                               table-info (when table-ref-node
                                            (let [inner (second table-ref-node)]
                                              (if (= :qualified-name (first inner))
                                                (let [qparts (vec (rest inner))]
                                                  (if (= 2 (count qparts))
                                                    {:schema (second (first qparts)) :table (second (second qparts))}
                                                    {:table (second (first qparts))}))
                                                {:table (second inner)})))]
                           table-info))
              after-load-node (first (filter #(= :after-load-do (first %)) children))
              after-load (when after-load-node
                           (let [commands (rest (second after-load-node))]
                             (mapv #(second %) (filter vector? commands))))
              cols (when source-col-list
                     (mapv :name (map parse-column-item
                                      (filter #(= :column-item (first %))
                                              (rest source-col-list)))))]
          (->LoadCommand
           :copy
           (merge source
                  {:columns (or cols [])})
           {:type :pgsql :target-uri pg-uri
            :schema (or (:schema tt-table) target-schema)
            :table (or (:table tt-table) target-table)}
           (merge {:truncate false}
                  (when (and target-schema target-table)
                    {:target-schema target-schema
                     :target-table target-table})
                  (when (and (nil? target-table) (:table pg-uri))
                    {:target-schema (or (:schema pg-uri) "public")
                     :target-table (:table pg-uri)})
                  (when tt-table
                    {:target-schema (:schema tt-table)
                     :target-table (:table tt-table)})
                  copy-options)
           (seq set-params) nil nil nil nil (seq before-load) (seq after-load) nil nil nil nil))

        :load-dbf
        (let [children (mapcat (fn [c] (if (= :load-dbf-clause (first c)) (rest c) [c])) (rest tree))
              dbf-node (first (filter #(= :dbf-source (first %)) children))
              source-enc (some #(when (= :source-encoding (first %))
                                  (let [v (second %)]
                                    (if (string? v) v (second v))))
                               children)
              source (when dbf-node
                       ;; dbf-source now wraps either dbf-filepath or http-source
                       (let [inner (second dbf-node)]
                         (case (first inner)
                           :http-source {:type :dbf :url  (second inner) :encoding source-enc}
                           :dbf-filepath {:type :dbf :path (second inner) :encoding source-enc}
                           ;; legacy: bare string (should not occur with new grammar)
                           {:type :dbf :path inner :encoding source-enc})))
              pg-uris  (filter #(= :pg-uri (first %)) children)
              pg-uri   (parse-uri (second (first pg-uris)))
              qualified (first (filter #(= :qualified-name (first %)) children))
              with-clause (first (filter #(= :with-dbf-clause (first %)) children))
              pg-uri-pos (first (keep-indexed #(when (= :pg-uri (first %2)) %1) children))
              source-col-list (when pg-uri-pos
                                (some #(when (= :column-list (first %)) %)
                                      (take pg-uri-pos children)))
              dbf-options (when with-clause
                            (let [opts (rest with-clause)]
                              (reduce merge {}
                                      (map hiccup->dbf-option
                                           (filter vector? opts)))))
              cast-rules (mapcat (fn [c]
                                   (when (and (vector? c) (= :cast-clause (first c)))
                                     (keep hiccup->cast-rule (rest c))))
                                 children)
              set-params (mapcat (fn [c]
                                   (when (and (vector? c) (= :set-clause (first c)))
                                     (keep hiccup->set-option (rest c))))
                                 children)
              before-load-node (first (filter #(= :before-load-do (first %)) children))
              before-load (when before-load-node
                            (let [commands (rest (second before-load-node))]
                              (mapv #(second %) (filter vector? commands))))
              target-table (when qualified
                             (let [parts (vec (rest qualified))]
                               (second (if (= 2 (count parts))
                                         (nth parts 1)
                                         (nth parts 0)))))
              target-schema (when (and qualified
                                       (= 2 (count (vec (rest qualified)))))
                              (-> qualified rest vec first second))
              target-table-clause (first (filter #(= :target-table-clause (first %)) children))
              tt-table (when target-table-clause
                         (let [parts (vec (rest target-table-clause))
                               table-ref-node (first (filter #(= :table-ref (first %)) parts))
                               table-info (when table-ref-node
                                            (let [inner (second table-ref-node)]
                                              (if (= :qualified-name (first inner))
                                                (let [qparts (vec (rest inner))]
                                                  (if (= 2 (count qparts))
                                                    {:schema (second (first qparts)) :table (second (second qparts))}
                                                    {:table (second (first qparts))}))
                                                {:table (second inner)})))]
                           table-info))
              after-load-node (first (filter #(= :after-load-do (first %)) children))
              after-load (when after-load-node
                           (let [commands (rest (second after-load-node))]
                             (mapv #(second %) (filter vector? commands))))
              cols (when source-col-list
                     (mapv :name (map parse-column-item
                                      (filter #(= :column-item (first %))
                                              (rest source-col-list)))))]
          (->LoadCommand
           :dbf
           (merge source
                  {:columns (or cols [])})
           {:type :pgsql :target-uri pg-uri
            :schema (or (:schema tt-table) target-schema)
            :table (or (:table tt-table) target-table)}
           (merge {:truncate false}
                  (when (and target-schema target-table)
                    {:target-schema target-schema
                     :target-table target-table})
                  (when (and (nil? target-table) (:table pg-uri))
                    {:target-schema (or (:schema pg-uri) "public")
                     :target-table (:table pg-uri)})
                  (when tt-table
                    {:target-schema (:schema tt-table)
                     :target-table (:table tt-table)})
                  dbf-options)
           (seq set-params) (seq cast-rules) nil nil nil (seq before-load) (seq after-load) nil nil nil nil))

        :load-database
        (let [children (mapcat (fn [c] (if (= :load-database-clause (first c)) (rest c) [c])) (rest tree))
              source-uri (parse-uri (second (first (filter #(= :source-uri (first %)) children))))
              pg-uri   (parse-uri (second (first (filter #(= :pg-uri (first %)) children))))
              db-options (mapcat (fn [c]
                                   (when (and (vector? c) (= :with-db-clause (first c)))
                                     (map hiccup->option-keyword (rest c))))
                                 children)
              set-params (mapcat (fn [c]
                                   (when (and (vector? c) (= :set-clause (first c)))
                                     (keep hiccup->set-option (rest c))))
                                 children)
              cast-rules (mapcat (fn [c]
                                   (when (and (vector? c) (= :cast-clause (first c)))
                                     (keep hiccup->cast-rule (rest c))))
                                 children)
              alter-schema-nodes (filter #(= :alter-schema-clause (first %)) children)
              alter-schemas (seq (map (fn [node]
                                        (let [parts (rest node)
                                              from-inner (second (first parts))
                                              to-inner (second (second parts))]
                                          {:source-name (if (vector? from-inner) (second from-inner) from-inner)
                                           :target-name   (if (vector? to-inner) (second to-inner) to-inner)}))
                                      alter-schema-nodes))
              parse-pattern (fn [pat-node]
                              (let [child (second pat-node)]
                                (if (string? child)
                                    ;; Regex pattern: ~/pattern/
                                  (str/replace child #"^/(.*)/$" "$1")
                                    ;; Quoted-string pattern: 'pattern'
                                  (let [raw (second child)]
                                    (-> raw
                                        (str/replace "%" ".*")
                                        (str/replace "_" "."))))))
              parse-patterns (fn [node]
                               ;; Collect all table-name-pattern leaves, unwrapping the
                               ;; table-name-pattern-list wrapper (#1328: multiple patterns)
                               (let [children (rest node)
                                     pat-list-node (first (filter #(= :table-name-pattern-list (first %)) children))
                                     pat-nodes (if pat-list-node
                                                 (filter #(= :table-name-pattern (first %)) (rest pat-list-node))
                                                 (filter #(= :table-name-pattern (first %)) children))]
                                 (mapv parse-pattern pat-nodes)))
              including-nodes (filter #(= :including-only (first %)) children)
              inc-patterns (mapcat parse-patterns including-nodes)
              excluding-nodes (filter #(= :excluding-only (first %)) children)
              exc-patterns (mapcat parse-patterns excluding-nodes)
              filters (cond-> {}
                        (seq inc-patterns) (assoc :including (vec inc-patterns))
                        (seq exc-patterns) (assoc :excluding (vec exc-patterns)))
              before-load-node (first (filter #(= :before-load-do (first %)) children))
              before-load (when before-load-node
                            (let [commands (rest (second before-load-node))]
                              (mapv #(second %) (filter vector? commands))))
              after-load-node (first (filter #(= :after-load-do (first %)) children))
              after-load (when after-load-node
                           (let [commands (rest (second after-load-node))]
                             (mapv #(second %) (filter vector? commands))))
              matview-node (first (filter #(= :materialize-views (first %)) children))
              materialize-views (when matview-node
                                  (let [inner (second matview-node)]
                                    (case (first inner)
                                      :materialize-all-views :all
                                      :materialize-named-views
                                      (let [defs (rest (second inner))]
                                        (mapv (fn [d]
                                                (let [parts (rest d)
                                                      name (second (first parts))
                                                      query (when (> (count parts) 2)
                                                              (second (nth parts 2)))]
                                                  (if query
                                                    {:name name :query query}
                                                    {:name name})))
                                              (filter vector? defs))))))
              alter-table-nodes (filter #(= :alter-table-clause (first %)) children)
              alter-table-rules (when (seq alter-table-nodes)
                                  (mapv (fn [node]
                                          (let [parts (rest node)
                                                pat-list (second (first parts))
                                                patterns (mapv (fn [p]
                                                                 (let [v (second p)]
                                                                   (if (string? v)
                                                                       ;; regex ~/pattern/: captured as bare string "/pat/"
                                                                     {:type :regex :value (str/replace v #"^/(.*)/$" "$1")}
                                                                       ;; 'quoted-string': captured as [:quoted-string "val"]
                                                                     {:type :exact :value (second v)})))
                                                               (filter #(= :table-name-pattern (first %)) (rest (first parts))))
                                                action-node (second (second parts))]
                                            {:patterns patterns
                                             :action
                                             (case (first action-node)
                                               :alter-table-set-schema  {:type :set-schema  :schema (second (second action-node))}
                                               :alter-table-rename      (let [v (second action-node)]
                                                                          {:type :rename :to (if (string? v) v (second v))})
                                               :alter-table-set-params  {:type :set-params  :params (second action-node)}
                                               :alter-table-set-tablespace {:type :set-tablespace :tablespace (second (second action-node))}
                                               {:type :unknown})}))
                                        alter-table-nodes))
              distribute-section-nodes (filter #(= :distribute-section (first %)) children)
              distribute-rules (or (seq (mapcat hiccup->distribute-rules distribute-section-nodes))
                                   [])
              decoding-as-nodes (filter #(= :decoding-as-clause (first %)) children)
              decoding-as (seq (keep hiccup->decoding-as decoding-as-nodes))]
          (->LoadCommand
           :database
           (cond-> source-uri
             (seq inc-patterns) (assoc :table-pattern (first inc-patterns)))
           {:type :pgsql :target-uri pg-uri}
           (into {} (keep (fn [k]
                            (cond
                              (keyword? k) [k true]
                              (vector? k)  k
                              :else nil))
                          db-options))
           (seq set-params)
           (seq cast-rules)
           (not-empty filters) alter-schemas (seq alter-table-rules) (seq before-load) (seq after-load)
           (if (= :all materialize-views) :all (seq materialize-views))
           distribute-rules decoding-as nil))

        :load-fixed
        (let [children   (mapcat (fn [c] (if (= :load-fixed-clause (first c)) (rest c) [c])) (rest tree))
              from-node  (first (filter #(= :fixed-from-source (first %)) children))
              source-enc (some #(when (= :source-encoding (first %))
                                  (let [v (second %)]
                                    (if (string? v) v (second v))))
                               children)
              ;; Parse the fixed-from-source
              from-source
              (when from-node
                (let [inner (second from-node)]
                  (case (first inner)
                    :fixed-filepath
                    {:type :fixed :path (second inner) :encoding source-enc}
                    :inline-source
                    {:type :fixed :inline true :encoding source-enc}
                    :fixed-filename-matching
                    {:type :fixed :filename-pattern (second (second inner)) :encoding source-enc}
                    {:type :fixed :path nil})))
              ;; Parse field specs
              field-list-node (first (filter #(= :fixed-field-list (first %)) children))
              fields
              (when field-list-node
                (vec (for [f (filter #(= :fixed-field (first %)) (rest field-list-node))]
                       (let [fchildren  (rest f)
                             col-name   (second (first (filter #(= :column-name (first %)) fchildren)))
                             int-nodes  (filter #(= :integer (first %)) fchildren)
                             start      (Integer/parseInt (second (first int-nodes)))
                             length     (Integer/parseInt (second (second int-nodes)))
                             opt-groups (filter #(= :fixed-field-option-group (first %)) fchildren)
                             ;; each opt-group child is [:fixed-field-option [:trim-right-ws]] — unwrap
                             all-inner  (mapcat (fn [g]
                                                  (map (fn [n]
                                                         (if (= :fixed-field-option (first n))
                                                           (second n)
                                                           n))
                                                       (rest g)))
                                                opt-groups)
                             trim-right (boolean (some #(= :trim-right-ws (first %)) all-inner))
                             null-blank (boolean (some #(= :null-if-blanks-opt (first %)) all-inner))]
                         {:name           col-name
                          :start          start
                          :length         length
                          :trim-right     trim-right
                          :null-if-blanks null-blank}))))
              ;; WITH clause
              with-clause (first (filter #(= :with-fixed-clause (first %)) children))
              fixed-options
              (when with-clause
                (reduce (fn [acc n]
                          (if (and (vector? n) (= :fixed-option (first n)))
                            (let [opt (second n)]
                              (case (first opt)
                                :truncate         (assoc acc :truncate true)
                                :create-tables    (assoc acc :create-tables true)
                                :create-no-tables (assoc acc :create-tables false)
                                :fixed-header     (assoc acc :fixed-header true)
                                :drop-indexes     (assoc acc :drop-indexes true)
                                :disable-triggers (assoc acc :disable-triggers true)
                                acc))
                            acc))
                        {}
                        (rest with-clause)))
              pg-uris    (filter #(= :pg-uri (first %)) children)
              pg-uri     (when (seq pg-uris) (parse-uri (second (first pg-uris))))
              ;; column-list after the INTO URI selects which fixed fields to load
              pg-uri-pos (first (keep-indexed #(when (= :pg-uri (first %2)) %1) children))
              post-into-col-list (when pg-uri-pos
                                   (some #(when (= :column-list (first %)) %)
                                         (drop (inc pg-uri-pos) children)))
              select-columns (when post-into-col-list
                               (vec (keep #(when (= :column-item (first %))
                                             (second (second %)))
                                          (rest post-into-col-list))))
              qualified  (first (filter #(= :qualified-name (first %)) children))
              target-table-clause (first (filter #(= :target-table-clause (first %)) children))
              tt-table   (when target-table-clause
                           (let [parts (vec (rest target-table-clause))
                                 table-ref-node (first (filter #(= :table-ref (first %)) parts))
                                 col-list (first (filter #(= :target-column-def-list (first %)) parts))
                                 table-info (when table-ref-node
                                              (let [inner (second table-ref-node)]
                                                (if (= :qualified-name (first inner))
                                                  (let [qparts (vec (rest inner))]
                                                    (if (= 2 (count qparts))
                                                      {:schema (second (first qparts))
                                                       :table  (second (second qparts))}
                                                      {:table (second (first qparts))}))
                                                  {:table (second inner)})))
                                 proj (when col-list
                                        {:projections (vec (keep :projection (map parse-target-column-def (rest col-list))))})]
                             (merge table-info proj)))
              target-table  (or (:table tt-table)
                                (when qualified
                                  (let [parts (vec (rest qualified))]
                                    (second (if (= 2 (count parts)) (nth parts 1) (nth parts 0))))))
              target-schema (or (:schema tt-table)
                                (when (and qualified (= 2 (count (vec (rest qualified)))))
                                  (-> qualified rest vec first second)))
              set-params (mapcat (fn [c]
                                   (when (and (vector? c) (= :set-clause (first c)))
                                     (keep hiccup->set-option (rest c))))
                                 children)
              before-load-node (first (filter #(= :before-load-do (first %)) children))
              before-load (when before-load-node
                            (mapv #(second %) (filter vector? (rest (second before-load-node)))))
              after-load-node  (first (filter #(= :after-load-do (first %)) children))
              after-load  (when after-load-node
                            (mapv #(second %) (filter vector? (rest (second after-load-node)))))]
          (->LoadCommand
           :fixed
           (merge from-source
                  {:fields fields}
                  (when (and inline-data (:inline from-source))
                    {:inline-data inline-data}))
           {:type :pgsql :target-uri pg-uri
            :schema target-schema
            :table  target-table}
           (merge {:create-tables false :truncate false}
                  (when target-table
                    {:target-schema (or target-schema "public")
                     :target-table  target-table})
                  (when (and (nil? target-table) pg-uri (:table pg-uri))
                    {:target-schema (or (:schema pg-uri) "public")
                     :target-table  (:table pg-uri)})
                  (when tt-table
                    (merge {:target-schema (or (:schema tt-table) "public")
                            :target-table  (:table tt-table)}
                           (when (:projections tt-table)
                             {:projections (:projections tt-table)})))
                  (when (seq select-columns)
                    {:select-columns select-columns})
                  fixed-options)
           (seq set-params) nil nil nil nil (seq before-load) (seq after-load) nil nil nil nil))

        :load-archive
        (let [children      (rest tree)
              from-node     (first (filter #(= :archive-from-source (first %)) children))
              archive-source
              (when from-node
                (let [inner (second from-node)]
                  (case (first inner)
                    :http-source {:type :http :url (second inner)}
                    :filepath    {:type :file :path (second inner)}
                    {:type :http :url (second inner)})))
              pg-uris       (filter #(= :pg-uri (first %)) children)
              pg-uri        (when (seq pg-uris) (parse-uri (second (first pg-uris))))
              ;; Flatten load-archive-clause wrappers
              flat-children (mapcat (fn [c]
                                      (if (= :load-archive-clause (first c)) (rest c) [c]))
                                    children)
              before-load-node (first (filter #(= :before-load-do (first %)) flat-children))
              before-load   (when before-load-node
                              (mapv #(second %) (filter vector? (rest (second before-load-node)))))
              after-load-node  (first (filter #(= :after-load-do (first %)) flat-children))
              after-load    (when after-load-node
                              (mapv #(second %) (filter vector? (rest (second after-load-node)))))
              ;; Sub-commands: load-fixed, load-csv, load-dbf nodes inside archive-sub-command
              sub-nodes     (filter #(= :archive-sub-command (first %)) children)
              commands      (vec (keep (fn [sn]
                                         (let [inner (second sn)]
                                           (transform inner inline-data)))
                                       sub-nodes))]
          (->LoadCommand
           :archive
           archive-source
           (when pg-uri {:type :pgsql :target-uri pg-uri})
           {}
           nil nil nil nil nil
           (seq before-load) (seq after-load)
           nil nil nil commands))

        ;; Default: try to transform the first child if this is a wrapper
        (if (seq (rest tree))
          (transform (second tree))
          (throw (ex-info (str "Unknown command tag: " tag) {:tag tag})))))))

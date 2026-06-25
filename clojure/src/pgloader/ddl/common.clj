(ns pgloader.ddl.common
  (:require [hugsql.core :as hugsql]
            [pgloader.cast :as cast]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))

(set! *warn-on-reflection* true)

(defn identifier-quote
  "Quote a PostgreSQL identifier."
  [^String s]
  (str "\"" (str/replace s "\"" "\"\"") "\""))

(defn quote-fqname
  "Format a fully-qualified name with quoting."
  ([table] (identifier-quote table))
  ([schema table]
   (str (identifier-quote schema) "." (identifier-quote table))))

(defn- pg-type-for
  "Map a MySQL type name to PostgreSQL type.
   Preserves precision modifiers for temporal types (#1629)."
  [^String mysql-type ^String extra]
  (let [upper (str/upper-case mysql-type)
        lower (str/lower-case mysql-type)
        unsigned? (str/includes? lower "unsigned")
        ;; Extract precision modifier like (6) from datetime(6)
        typemod (re-find #"\(\d+\)" mysql-type)]
    (cond
      ;; Pass-through: native PostgreSQL types emitted by non-MySQL sources
      (= lower "uuid")   "uuid"
      (= lower "xml")    "xml"
      ;; Pass-through array types from PostgreSQL sources (#1371)
      (str/ends-with? lower "[]") lower
      (str/starts-with? upper "BIGSERIAL")   "bigserial"
      ;; Unsigned integers: upcast to avoid overflow (matches CL cast rules)
      (and unsigned? (str/starts-with? upper "TINYINT"))   "smallint"
      (and unsigned? (str/starts-with? upper "SMALLINT"))  "integer"
      (and unsigned? (str/starts-with? upper "MEDIUMINT")) "integer"
      (and unsigned? (str/starts-with? upper "BIGINT"))    "numeric"
      (and unsigned? (or (str/starts-with? upper "INTEGER")
                         (str/starts-with? upper "INT")))  "bigint"
      ;; Signed integers
      (str/starts-with? upper "BIGINT")      "bigint"
      (= lower "tinyint(1)")                 "boolean"
      (str/starts-with? upper "TINYINT")     "smallint"
      (str/starts-with? upper "SMALLINT")    "smallint"
      (str/starts-with? upper "MEDIUMINT")   "integer"
      ;; int(N) with N>=10 → bigint (matches CL cast rule: typemod >= 10 → bigint)
      (and (str/starts-with? upper "INT")
           (when-let [m (re-find #"\((\d+)\)" mysql-type)]
             (>= (Integer/parseInt (second m)) 10)))
      "bigint"
      (str/starts-with? upper "INT")         "integer"
      (str/starts-with? upper "REAL")        "real"
      (str/starts-with? upper "FLOAT")       "double precision"
      (str/starts-with? upper "DOUBLE")      "double precision"
      (str/starts-with? upper "DECIMAL")     "numeric"
      (str/starts-with? upper "NUMERIC")     "numeric"
      (str/starts-with? upper "BOOLEAN")     "boolean"
      (str/starts-with? upper "BIT")         "bit varying"
      ;; MySQL char(N) → varchar(N): preserves the length constraint without
      ;; PostgreSQL's blank-padding semantics. Users can override to text
      ;; with a cast rule: type char to text drop typemod
      (= upper "CHAR")                       "varchar(1)"
      (str/starts-with? upper "CHAR")
      (str "varchar" (or (re-find #"\(\d+\)" mysql-type) "(1)"))
      (str/starts-with? upper "VARCHAR")     "varchar"
      (str/starts-with? upper "BINARY")      "bytea"
      (str/starts-with? upper "VARBINARY")   "bytea"
      (str/starts-with? upper "TINYBLOB")    "bytea"
      (str/starts-with? upper "BLOB")        "bytea"
      (str/starts-with? upper "MEDIUMBLOB")  "bytea"
      (str/starts-with? upper "LONGBLOB")    "bytea"
      (str/starts-with? upper "TINYTEXT")    "text"
      (str/starts-with? upper "TEXT")        "text"
      (str/starts-with? upper "MEDIUMTEXT")  "text"
      (str/starts-with? upper "LONGTEXT")    "text"
      (str/starts-with? upper "ENUM")        "text"
      (str/starts-with? upper "SET")         "text[]"
      (str/starts-with? upper "JSON")        "jsonb"
      ;; Preserve precision modifier for temporal types: datetime(6) → timestamptz(6) (#1629)
      (str/starts-with? upper "TIMESTAMP")   (str "timestamptz" (or typemod ""))
      (str/starts-with? upper "DATETIME")    (str "timestamptz" (or typemod ""))
      (str/starts-with? upper "DATE")        "date"
      (str/starts-with? upper "TIME")        (str "time" (or typemod ""))
      (str/starts-with? upper "YEAR")        "smallint"
      ;; Spatial types — requires PostGIS on the target.
      ;; The docker-compose postgres image includes PostGIS.
      (str/starts-with? upper "GEOMETRY")         "geometry"
      (str/starts-with? upper "POINT")            "geometry(Point)"
      (str/starts-with? upper "LINESTRING")       "geometry(LineString)"
      (str/starts-with? upper "POLYGON")          "geometry(Polygon)"
      (str/starts-with? upper "MULTIPOINT")       "geometry(MultiPoint)"
      (str/starts-with? upper "MULTILINESTRING")  "geometry(MultiLineString)"
      (str/starts-with? upper "MULTIPOLYGON")     "geometry(MultiPolygon)"
      (str/starts-with? upper "GEOMETRYCOLLECTION") "geometry(GeometryCollection)"
      :else "text")))

(defn- coerce-default-for-type
  "Coerce a column default to be valid for the given PostgreSQL type.
   If the column has a :cast-fn (from a user cast rule), apply it — this
   mirrors CL's format-default-value which runs the transform on the default.
   Otherwise fall back to mapping 0/1 for boolean columns."
  [^String default ^String pg-type cast-fn]
  (when default
    (if cast-fn
      ;; Apply the cast transform to the default value, same as CL does.
      ;; tinyint-to-boolean('3') → 't', tinyint-to-boolean('0') → 'f', etc.
      (cast/apply-cast cast-fn default)
      (if (re-find #"(?i)^bool" (or pg-type ""))
        (case (str/lower-case (str/trim default))
          ("0" "false") "f"
          ("1" "true")  "t"
          default)
        default))))

(defn- format-default
  "Format a default value, quoting bare string literals for PostgreSQL.
   Handles:
   - MySQL bit literals b'0'/b'1' → pass through (#1280)
   - current_timestamp(N) with precision → current_timestamp (#1403)
   - backslash in string defaults → escape as '' (#1546)"
  [^String val]
  (let [upper (str/upper-case val)
        ;; Strip trailing () or (N) for comparison (PG doesn't accept those on keywords)
        stripped (str/replace upper #"\(\d*\)$" "")]
    (cond
      ;; Numeric: don't quote
      (re-matches #"^-?\d+(\.\d+)?$" val) val
      ;; Bit literal b'0', b'1', B'0', B'1': pass through as-is (#1280)
      (re-matches #"(?i)^b'[01]+'$" val) val
      ;; Hex literal X'0F': pass through as-is (valid PG syntax)
      (re-matches #"(?i)^[X]'.*'$" val) val
      ;; Known PostgreSQL keywords/functions: don't quote, strip precision (#1403)
      (#{"CURRENT_TIMESTAMP" "CURRENT_DATE" "CURRENT_TIME"
         "LOCALTIMESTAMP" "LOCALTIME" "TRUE" "FALSE"
         "NOW"}
       stripped) (str/lower-case stripped)
      ;; Bare identifier (word): quote as string literal with backslash escaping (#1546)
      (re-matches #"^\w+$" val) (str "'" (str/replace val "'" "''") "'")
      ;; Anything else: quote as string literal; escape single quotes and backslashes (#1546)
      :else (str "'"
                 (-> val
                     (str/replace "\\" "\\\\")
                     (str/replace "'" "''"))
                 "'"))))

(defn- strip-quotes
  "Strip surrounding single or double quotes, repeatedly, until stable."
  [^String s]
  (loop [s s]
    (let [stripped (if (and (>= (count s) 2)
                            (or (and (str/starts-with? s "'") (str/ends-with? s "'"))
                                (and (str/starts-with? s "\"") (str/ends-with? s "\""))))
                     (subs s 1 (dec (count s)))
                     s)]
      (if (= stripped s) s (recur stripped)))))

(defn- zero-date?
  "Check if a column default is a MySQL zero-date/zero-datetime value."
  [col]
  (when-let [d (str (:column-default col))]
    (re-matches #"(?i)0000[-/]00[-/]00.*" (strip-quotes (str/trim d)))))

(defn column-def
  "Generate a PostgreSQL column definition string.
   Omits NOT NULL for columns with auto_increment or default NULL.
   Emits GENERATED ALWAYS AS (expr) STORED for columns with :generated-expression."
  [col]
  (let [{:keys [column-name column-type column-default is-nullable extra
                generated-expression]} col
        ;; Use column-type directly only when a cast rule actually changed the type
        ;; (i.e., :source-column-type differs from :column-type, meaning :target-type
        ;; was applied). When source equals column-type, no target was set; still call
        ;; pg-type-for so MySQL type names (datetime, etc.) map to their PG equivalents.
        src-type (:source-column-type col)
        pg-type (if (and src-type (not= src-type column-type))
                  column-type
                  (pg-type-for (or src-type column-type) extra))
        quoted-name (identifier-quote column-name)]
    (if generated-expression
      ;; PostgreSQL only supports STORED generated columns (no VIRTUAL).
      ;; Both MySQL VIRTUAL and STORED generated columns become STORED here.
      (str "  " quoted-name " " pg-type
           " GENERATED ALWAYS AS (" generated-expression ") STORED")
      (let [zero? (zero-date? col)
            ;; MariaDB information_schema returns the string "NULL" (not SQL NULL)
            ;; for columns with no explicit default. Filter it out before calling
            ;; coerce-default-for-type so cast functions are not applied to "NULL".
            raw-default (when (and column-default (not= "NULL" (str column-default)))
                          (strip-quotes (str column-default)))
            coerced-default (coerce-default-for-type raw-default pg-type (:cast-fn col))
            temporal-int-default? (and coerced-default
                                       (re-find #"(?i)^(timestamp|date|time)" (or pg-type ""))
                                       (re-matches #"^-?\d+$" (str coerced-default)))
            default-str (when (and coerced-default
                                   (not= "NULL" (str coerced-default))
                                   (not= "" (str coerced-default))
                                   (not zero?)
                                   (not temporal-int-default?))
                          (str " DEFAULT " (format-default coerced-default)))]
        (str "  " quoted-name " " pg-type
             (when (and (false? is-nullable)
                        (not (str/includes? (str extra) "auto_increment"))
                        (not= "NULL" (str column-default))
                        (not zero?))
               " NOT NULL")
             default-str)))))

(defn table-not-null-check
  "Check if we should include a WHEN condition for NOT NULL validation."
  [col pg-type]
  (let [{:keys [column-name column-default is-nullable]} col]
    (when (and (false? is-nullable)
               (not= "NULL" (str column-default))
               (not (zero-date? col)))
      [column-name pg-type])))

(defn create-table-sql
  "Generate a CREATE TABLE statement from column definitions.
   Primary key is NOT added inline — caller should follow up with
   create-primary-key-sql after querying the table OID.
   Optional table-comment emits COMMENT ON TABLE."
  ([schema table-name columns] (create-table-sql schema table-name columns nil))
  ([schema table-name columns table-comment]
   (let [quoted-name (quote-fqname schema table-name)
         col-defs (str/join ",\n" (map column-def columns))
         tbl-comment (when (not-empty table-comment)
                       (str "COMMENT ON TABLE " quoted-name
                            " IS '" (str/replace table-comment "'" "''") "';"))
         col-comments (str/join "\n"
                                (keep (fn [col]
                                        (when-let [c (not-empty (:column-comment col))]
                                          (str "COMMENT ON COLUMN " quoted-name "."
                                               (identifier-quote (:column-name col))
                                               " IS '" (str/replace c "'" "''") "';")))
                                      columns))
         comments (str/join "\n" (filter not-empty [tbl-comment col-comments]))]
     (str "CREATE TABLE IF NOT EXISTS " quoted-name " (\n" col-defs "\n);\n"
          (when (seq comments) (str comments "\n"))))))

(defn create-primary-key-sql
  "Generate idx_{oid}_PRIMARY index + ALTER TABLE ADD PRIMARY KEY USING INDEX.
   This matches CL pgloader's naming convention."
  [schema table-name primary-key oid]
  (when (seq primary-key)
    (let [quoted-table (quote-fqname schema table-name)
          idx-name     (str "idx_" oid "_PRIMARY")
          quoted-idx   (identifier-quote idx-name)
          cols         (str/join ", " (map identifier-quote primary-key))]
      [(str "CREATE UNIQUE INDEX " quoted-idx " ON " quoted-table " (" cols ");")
       (str "ALTER TABLE " quoted-table " ADD PRIMARY KEY USING INDEX " quoted-idx ";")])))

(defn drop-table-if-exists-sql
  "Generate DROP TABLE IF EXISTS statement."
  [schema table-name]
  (str "DROP TABLE IF EXISTS " (quote-fqname schema table-name) " CASCADE;"))

(defn create-index-sql
  "Generate CREATE INDEX statements for columns marked as keys."
  [schema table-name columns]
  (let [quoted-table (quote-fqname schema table-name)]
    (str/join "\n"
              (keep (fn [col]
                      (when (:key col)
                        (let [idx-name (str table-name "_" (:column-name col) "_idx")
                              quoted-col (identifier-quote (:column-name col))]
                          (str "CREATE INDEX IF NOT EXISTS "
                               (identifier-quote idx-name)
                               " ON " quoted-table " (" quoted-col ");\n"))))
                    columns))))

(defn create-indexes-sql
  "Generate CREATE INDEX statements from the catalog :indexes vector.
   Each index entry: {:name string, :unique bool, :columns [string]}
   When oid is provided, names indexes as idx_{oid}_{name} matching CL pgloader.

   Returns a vector of SQL strings (one per index)."
  [schema table-name indexes & [oid]]
  (let [quoted-table (quote-fqname schema table-name)]
    (mapv (fn [idx]
            (let [raw-name  (:name idx)
                  idx-name  (if oid
                              (identifier-quote (str "idx_" oid "_" raw-name))
                              (identifier-quote raw-name))
                  fulltext? (= "FULLTEXT" (:index-type idx))
                  uniq-str  (if (and (:unique idx) (not fulltext?)) " UNIQUE" "")
                  quoted-cols (str/join ", "
                                        (map (fn [col]
                                               (if (str/starts-with? col "(")
                                                 col
                                                 (identifier-quote col)))
                                             (:columns idx)))]
              (if fulltext?
                (str "CREATE INDEX IF NOT EXISTS "
                     idx-name " ON " quoted-table
                     " USING gin(to_tsvector('simple', " quoted-cols "));\n")
                (str "CREATE" uniq-str " INDEX IF NOT EXISTS "
                     idx-name " ON " quoted-table " (" quoted-cols ")"
                     (when-let [w (:where idx)] (str " WHERE " w))
                     ";\n"))))
          indexes)))

(defn create-fkeys-sql
  "Generate ALTER TABLE ADD FOREIGN KEY statements from :fkeys vector.
   Each fkey entry: {:name string, :columns [string], :ftable string,
                     :fcols [string], :on-delete string, :on-update string}

   Returns a vector of SQL strings (one per FK)."
  [schema table-name fkeys]
  (mapv (fn [fk]
          (let [quoted-table  (quote-fqname schema table-name)
                quoted-ftable (quote-fqname schema (:ftable fk))
                fk-name       (identifier-quote (:name fk))
                cols          (str/join ", " (map identifier-quote (:columns fk)))
                fcols         (str/join ", " (map identifier-quote (:fcols fk)))
                on-upd        (when (:on-update fk)
                                (str " ON UPDATE " (:on-update fk)))
                on-del        (when (:on-delete fk)
                                (str " ON DELETE " (:on-delete fk)))]
            (str "ALTER TABLE " quoted-table
                 " ADD CONSTRAINT " fk-name
                 " FOREIGN KEY (" cols ")"
                 " REFERENCES " quoted-ftable " (" fcols ")"
                 on-upd on-del ";\n")))
        fkeys))

(def ^:private pg-max-identifier-length 63)

(defn- snake-case-transform
  "Convert an identifier to snake_case:
   1. Insert underscore between camelCase boundaries (Foo → foo, FooBar → foo_bar).
   2. Replace spaces and hyphens with underscore.
   3. Collapse consecutive underscores (Object_Name → object_name, not object__name).
   4. Replace $ with _ ($ is valid in MySQL/SQLite but not meaningful in PG identifiers).
   5. Lowercase the whole result.
   6. Truncate to 63 bytes (PostgreSQL max identifier length) with a warning when trimmed."
  [^String s]
  (let [result (-> s
                   ;; camelCase → snake_case: insert _ between lower→upper transitions
                   (str/replace #"([a-z\d])([A-Z])" "$1_$2")
                   ;; Handle runs of uppercase followed by lowercase: XMLParser → xml_parser
                   (str/replace #"([A-Z]+)([A-Z][a-z])" "$1_$2")
                   ;; Replace spaces, hyphens, $ with underscore
                   (str/replace #"[\s\-$]+" "_")
                   str/lower-case
                   ;; Collapse consecutive underscores (e.g. Object_Name → object_name)
                   (str/replace #"_+" "_")
                   ;; Strip leading/trailing underscores that may have appeared
                   (str/replace #"^_+|_+$" ""))]
    (if (> (count result) pg-max-identifier-length)
      (do (log/warn (str "Identifier truncated to " pg-max-identifier-length
                         " characters: " result))
          (subs result 0 pg-max-identifier-length))
      result)))

(defn apply-identifier-case
  "Transform identifiers in the catalog according to with-options.
   Matches CL pgloader behaviour:
   - Default (no option): preserve original case (quote identifiers).
   - :quote-ids           preserve original case (same as default).
   - :downcase-ids        explicit lowercase.
   - :snake-case-ids      camelCase → snake_case + lowercase."
  [catalog with-options]
  (let [downcase? (get with-options :downcase-ids false)
        snake?    (get with-options :snake-case-ids false)]
    (if (not (or downcase? snake?))
      catalog
      (mapv (fn [t]
              (let [xf (if snake?
                         snake-case-transform
                         str/lower-case)]
                (-> t
                    (update :table-name xf)
                    (update :schema     xf)
                    (update :columns
                            (fn [cols]
                              (mapv #(update % :column-name xf) cols)))
                    (update :primary-key
                            (fn [pk] (mapv xf pk)))
                    (update :indexes
                            (fn [idxes]
                              (mapv (fn [idx]
                                      (-> idx
                                          (update :name xf)
                                          (update :columns (fn [cs] (mapv xf cs)))))
                                    idxes)))
                    (update :fkeys
                            (fn [fks]
                              (mapv (fn [fk]
                                      (-> fk
                                          (update :ftable xf)
                                          (update :columns (fn [cs] (mapv xf cs)))
                                          (update :fcols   (fn [cs] (mapv xf cs)))))
                                    fks))))))
            catalog))))

(defn apply-alter-schema
  "Apply ALTER SCHEMA ... RENAME TO ... rules from the load command.
   Each rule: {:source-name s :target-name t}
   Schemas not matched by any rule keep their original name (the MySQL
   database name), matching CL pgloader behaviour: MySQL DATABASE = PG SCHEMA."
  [catalog alter-schema-rules]
  (if (seq alter-schema-rules)
    (let [rename-map (into {} (map (juxt :source-name :target-name))
                           alter-schema-rules)]
      (mapv (fn [t]
              (assoc t :schema (get rename-map (:schema t) (:schema t))))
            catalog))
    catalog))

(defn- matches-pattern?
  "Check if table-name matches an alter-table pattern.
   Pattern is {:type :exact|:regex, :value string}."
  [table-name {:keys [type value]}]
  (case type
    :exact (= table-name value)
    :regex (boolean (re-find (re-pattern value) table-name))
    false))

(defn apply-alter-table
  "Apply ALTER TABLE NAMES MATCHING rules from the load command.
   Supported actions: :set-schema, :rename.
   Other actions (:set-params, :set-tablespace) are silently ignored.
   Rules are applied in order; each table is tested against all rules."
  [catalog alter-table-rules]
  (if (seq alter-table-rules)
    (mapv (fn [t]
            (reduce (fn [t {:keys [patterns action]}]
                      (if (some #(matches-pattern? (:table-name t) %) patterns)
                        (case (:type action)
                          :set-schema (assoc t :schema (:schema action))
                          :rename     (assoc t :table-name (:to action))
                          t)
                        t))
                    t
                    alter-table-rules))
          catalog)
    catalog))

(defn- parse-enum-values
  "Parse MySQL enum('a','b','c') or set('a','b') → [\"a\" \"b\" \"c\"]"
  [^String col-type]
  (when-let [m (re-find #"(?i)^(?:enum|set)\((.+)\)$" col-type)]
    (->> (re-seq #"'((?:[^']*(?:''[^']*)*)*)'" (second m))
         (mapv #(str/replace (second %) "''" "'")))))

(defn resolve-enum-type-name
  "Return the first candidate name not already taken in schema on the target.
   names-existing-fn is (fn [schema [name ...]] -> #{name ...}) — it receives
   all candidates at once and returns the set of taken names (one query).
   base-name is expected to end in '_t'; alternatives strip that suffix and
   try '_enum' suffix or 'enum_' prefix.  Throws when all candidates conflict."
  [names-existing-fn schema base-name]
  (let [stem       (if (str/ends-with? base-name "_t")
                     (subs base-name 0 (- (count base-name) 2))
                     base-name)
        candidates [base-name
                    (str stem "_enum")
                    (str "enum_" stem)]
        taken      (names-existing-fn schema candidates)]
    (or (first (remove taken candidates))
        (throw (ex-info (str "Cannot find a non-conflicting PostgreSQL type name for "
                             base-name " in schema " schema
                             "; tried: " (str/join ", " candidates))
                        {:schema schema :base-name base-name :tried candidates})))))

(defn add-enum-types
  "For columns with ENUM or SET type, compute PostgreSQL ENUM type names and
   attach them to the catalog. Returns updated catalog where:
   - :enum-types is a vector of {:type-name s :schema s :values [...] :is-set bool}
   - each ENUM/SET column's :column-type is updated to the PG type reference

   name-exists-fn is (fn [schema name] -> bool) checked against the target
   database; pass (constantly false) when no connection is available."
  [catalog name-exists-fn]
  (mapv (fn [t]
          (let [table (:table-name t)
                sch   (or (:schema t) "public")
                [new-cols enum-types]
                (reduce (fn [[cols types] col]
                          (let [ct    (str/lower-case (or (:column-type col) ""))
                                is-en (str/starts-with? ct "enum(")
                                is-st (str/starts-with? ct "set(")]
                            (if (or is-en is-st)
                              (let [values    (parse-enum-values (:column-type col))
                                    type-name (resolve-enum-type-name
                                               name-exists-fn sch
                                               (str table "_" (:column-name col) "_t"))
                                    pg-ref    (str (identifier-quote sch) "." (identifier-quote type-name))
                                    pg-type   (if is-st (str pg-ref "[]") pg-ref)]
                                [(conj cols (assoc col :column-type pg-type
                                                   :source-column-type (:column-type col)))
                                 (conj types {:type-name type-name
                                              :schema    sch
                                              :values    values
                                              :is-set    is-st})])
                              [(conj cols col) types])))
                        [[] []]
                        (:columns t))]
            (assoc t :columns new-cols :enum-types (vec enum-types))))
        catalog))

(defn create-check-constraints-sql
  "Generate ALTER TABLE ADD CONSTRAINT ... CHECK (...) statements.
   Each check entry: {:constraint-name s :check-clause s}
   MySQL 8.0.16+ exposes CHECK constraints in information_schema.CHECK_CONSTRAINTS.
   Returns a vector of SQL strings."
  [schema table-name checks]
  (into []
        (keep (fn [ck]
                (let [clause (:check-clause ck)]
                  (when (and (string? clause) (not (str/blank? clause)))
                    (let [;; MySQL 8 returns check clauses with backtick-quoted
                          ;; identifiers (e.g. `salary` > 0). Replace backtick
                          ;; quoting with PostgreSQL double-quote quoting so the
                          ;; clause is valid SQL in PostgreSQL.
                          pg-clause    (str/replace clause "`" "\"")
                          quoted-table (quote-fqname schema table-name)
                          ck-name      (identifier-quote (:constraint-name ck))]
                      (str "ALTER TABLE " quoted-table
                           " ADD CONSTRAINT " ck-name
                           " CHECK (" pg-clause ");"))))))
        checks))

(defn create-enum-types-sql
  "Generate DROP TYPE IF EXISTS + CREATE TYPE statements for ENUM/SET columns."
  [enum-types]
  (vec
   (mapcat (fn [et]
             (let [quoted (quote-fqname (:schema et) (:type-name et))
                   values (str/join ", "
                                    (map #(str "'" (str/replace % "'" "''") "'")
                                         (:values et)))]
               [(str "DROP TYPE IF EXISTS " quoted " CASCADE;")
                (str "CREATE TYPE " quoted " AS ENUM (" values ");")]))
           enum-types)))

(defn- on-update-current-timestamp?
  "Return truthy if the column has an ON UPDATE CURRENT_TIMESTAMP extra."
  [col]
  (when-let [e (:extra col)]
    (re-find #"(?i)on update current_timestamp" (str e))))

(defn create-triggers-sql
  "Generate trigger function + trigger for ON UPDATE CURRENT_TIMESTAMP columns.
   One trigger per table (covering all such columns), matching CL pgloader behaviour."
  [schema table-name columns]
  (let [ts-cols (filterv on-update-current-timestamp? columns)]
    (when (seq ts-cols)
      (let [quoted-table  (quote-fqname schema table-name)
            fn-name       (str "upd_" table-name "_ts")
            quoted-schema (identifier-quote schema)
            quoted-fn     (identifier-quote fn-name)
            trigger-name  (identifier-quote (str "trg_" table-name "_ts"))
            assignments   (str/join "\n    "
                                    (map #(str "NEW." (identifier-quote (:column-name %)) " = now();")
                                         ts-cols))
            fn-sql  (str "CREATE OR REPLACE FUNCTION "
                         quoted-schema "." quoted-fn "() RETURNS trigger AS $$\n"
                         "BEGIN\n"
                         "    " assignments "\n"
                         "    RETURN NEW;\n"
                         "END;\n"
                         "$$ LANGUAGE plpgsql;")
            trg-sql (str "CREATE TRIGGER " trigger-name
                         " BEFORE UPDATE ON " quoted-table
                         " FOR EACH ROW EXECUTE PROCEDURE "
                         quoted-schema "." quoted-fn "();")]
        [fn-sql trg-sql]))))

(defn reset-sequences-sql
  "Generate SELECT setval() for auto-increment columns.
   Calls pg_catalog.setval with MAX(col) to advance the sequence
   past any data that was bulk-loaded (bypassing the sequence).

   Returns a vector of SQL strings (one per auto-increment column)."
  [schema table-name columns]
  (let [quoted-fqname (quote-fqname schema table-name)]
    (vec (keep (fn [col]
                 (when (and (:column-name col)
                            (:extra col)
                            (str/includes? (str/lower-case (:extra col)) "auto_increment")
                            ;; Only reset sequences for integer-typed columns.
                            ;; Cast rules may change the PG type (e.g. int → text);
                            ;; non-integer MAX() causes COALESCE type mismatch.
                            (let [src     (or (:source-column-type col) (:column-type col) "")
                                  ct      (or (:column-type col) "")
                                  pg-type (str/lower-case
                                           (if (not= src ct)
                                             ct
                                             (pg-type-for ct nil)))]
                              (some #(str/starts-with? pg-type %)
                                    ["integer" "bigint" "bigserial" "smallint" "serial"
                                     "int2" "int4" "int8"])))
                   (let [col-name   (:column-name col)
                         quoted-col (identifier-quote col-name)]
                     (str "SELECT pg_catalog.setval("
                          "pg_get_serial_sequence('" quoted-fqname "', '" col-name "')"
                          ", COALESCE(MAX(" quoted-col "), 1), false)"
                          " FROM " quoted-fqname ";\n"))))
               columns))))

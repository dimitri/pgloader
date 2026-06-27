(ns pgloader.ddl.citus
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [hugsql.core :as hugsql]
            [pgloader.ddl.common :refer [identifier-quote]]))

(hugsql/def-db-fns "pgloader/ddl/citus.sql")

(set! *warn-on-reflection* true)

(defn execute-distribute!
  "Execute the Citus distribution call for one rule against CONN.
   rule is a map with :type, :table, :using, :schema."
  [conn {:keys [type table using schema]}]
  (let [schema (or schema "public")
        fqn    (str "\"" schema "\".\"" table "\"")]
    (case type
      :reference (exec-create-reference-table conn {:fqn fqn})
      (:distributed :distributed-from) (exec-create-distributed-table conn {:fqn fqn :col using}))))

(defn- prepend-if-absent
  "Prepend ITEM to vector COLL if not already present."
  [coll item]
  (if (some #{item} coll) coll (vec (cons item coll))))

(defn- build-join-clause
  "Generate JOIN clauses to traverse from TABLE-ENTRY through FROM-CHAIN.
   Uses :fkeys already present in each catalog entry — no new DB queries."
  [table-entry from-chain catalog-index]
  (str/join " "
            (loop [current table-entry
                   remaining from-chain
                   clauses []]
              (if (empty? remaining)
                clauses
                (let [next-table-name (first remaining)
                      next-entry      (get catalog-index next-table-name)
                      schema          (or (:schema next-entry) "public")
                      fkey (or (some #(when (= next-table-name (:ftable %)) %) (:fkeys current))
                               (throw (ex-info
                                       (str "Citus: No FK from '" (:table-name current)
                                            "' to '" next-table-name "' found in catalog. "
                                            "Available fkeys on '" (:table-name current) "': "
                                            (pr-str (mapv :ftable (:fkeys current)))
                                            ". Cannot build backfill JOIN for: "
                                            "DISTRIBUTE " (:table-name table-entry) " USING "
                                            (:column-name (first (:columns table-entry)))
                                            " FROM " (str/join ", " from-chain)
                                            ". Hint: add a foreign key in the source database, "
                                            "or specify an explicit JOIN in the FROM clause.")
                                       {:current (:table-name current)
                                        :target  next-table-name
                                        :rule    {:table (:table-name table-entry)
                                                  :from  from-chain}})))
                      on-clause (str/join " AND "
                                          (map (fn [lc rc]
                                                 (str (:table-name current) "." lc
                                                      " = " next-table-name "." rc))
                                               (:columns fkey)
                                               (:fcols fkey)))
                      clause (str "JOIN " (identifier-quote schema) "."
                                  (identifier-quote next-table-name)
                                  " ON " on-clause)]
                  (recur next-entry (rest remaining) (conj clauses clause)))))))

(defn- build-backfill-select
  "Build the full SELECT … FROM … JOIN … query for backfilling the distribution key.
   DIST-COL-NAME is the name of the distribution column.
   FROM-TABLE-NAME is the table the distribution column comes from.
   JOIN-SQL is the prebuilt JOIN clause string.
   TABLE-ENTRY is the source table catalog entry.
   SOURCE-TYPE is :mysql or :pgsql — controls ::text casting."
  [table-entry dist-col-name from-table-name join-sql source-type]
  (let [schema     (or (:schema table-entry) "public")
        tname      (:table-name table-entry)
        own-cols   (mapv :column-name (:columns table-entry))
        cast-text? (= :pgsql source-type)
        fmt-col    (fn [tbl col]
                     (if cast-text?
                       (str "CAST(" tbl "." col " AS text)")
                       (str tbl "." col)))
        col-list   (str/join ", "
                             (concat [(fmt-col from-table-name dist-col-name)]
                                     (map #(fmt-col tname %) own-cols)))]
    (str "SELECT " col-list
         " FROM " (identifier-quote schema) "." (identifier-quote tname)
         " " join-sql)))

(defn- check-fkeys-present
  "Verify that every FROM hop has an FK. Throws early with a clear message
   rather than waiting until build-join-clause fails mid-traversal.
   Returns the catalog-index for use by build-join-clause."
  [catalog from-chain source-table-name rule table-index]
  (loop [remaining from-chain
         current-table source-table-name]
    (when (seq remaining)
      (let [next-table (first remaining)
            entry (get table-index current-table)]
        (when-not (some #(= next-table (:ftable %)) (:fkeys entry))
          (throw (ex-info
                  (str "Citus: No FK from '" current-table "' to '" next-table
                       "' found in catalog. "
                       "Available fkeys on '" current-table "': "
                       (pr-str (mapv :ftable (:fkeys entry)))
                       ". Cannot build backfill JOIN for: "
                       "DISTRIBUTE " source-table-name " USING " (:using rule)
                       " FROM " (str/join ", " from-chain)
                       ". Hint: add a foreign key in the source database.")
                  {:current current-table :target next-table :rule rule})))
        (recur (rest remaining) next-table)))))

(defn compute-derived-rules
  "Walk the FK graph from a :distributed rule and derive implicit distribution
   rules for all dependent tables — mirroring CL pgloader's compute-foreign-rules.

   Algorithm: starting from `table` distributed on `using` (which must be in
   that table's primary key), follow reverse FK edges. For each table S that
   has an FK pointing to the current table T:
     - The root FK (first one added to the chain) maps the distribution key
       through its column list.
     - from-tables is the intermediate path S→…→T (excluding the root T).
   Returns a (possibly empty) seq of derived rule maps."
  [catalog {:keys [table using] :as _rule}]
  (let [table-index  (into {} (map (juxt :table-name identity) catalog))
        ;; reverse-fks: {target-table-name → [{:source source-name :fkey fkey-entry}]}
        reverse-fks  (reduce (fn [acc entry]
                               (reduce (fn [m fk]
                                         (update m (:ftable fk) (fnil conj [])
                                                 {:source (:table-name entry) :fkey fk}))
                                       acc (:fkeys entry)))
                             {} catalog)]
    ;; fkey-chain is a vector of FK maps, oldest (closest to root) first.
    ;; root-fk = (first fkey-chain) — the FK that connects root's PK to root+1.
    ;; visited tracks tables we have already processed to prevent cycles.
    (letfn [(walk [tname fkey-chain visited]
              (let [entry  (get table-index tname)
                    pk-set (set (:primary-key entry))]
                (when (contains? pk-set using)
                  (let [visited' (conj visited tname)]
                    (mapcat
                     (fn [{:keys [source fkey]}]
                       (when-not (contains? visited' source)
                         (let [new-chain   (conj fkey-chain fkey)
                               root-fk     (first new-chain)
                               dist-pos    (first (keep-indexed #(when (= using %2) %1)
                                                                (:fcols root-fk)))
                               local-col   (when dist-pos (nth (:columns root-fk) dist-pos))
                                ;; from-tables: intermediate hops in source→…→root order
                                ;; = ftables of all FKs after the root, reversed
                               from-tables (when (> (count new-chain) 1)
                                             (vec (reverse (mapv :ftable (rest new-chain)))))]
                           (when local-col
                             (let [new-rule (if (seq from-tables)
                                              {:type :distributed-from :table source
                                               :using local-col :from from-tables}
                                              {:type :distributed :table source
                                               :using local-col})]
                               (cons new-rule
                                     (walk source new-chain visited')))))))
                     (get reverse-fks tname []))))))]
      (walk table [] #{}))))

(defn expand-distribute-rules
  "Expand DISTRIBUTE rules by walking the FK graph and deriving implicit rules
   for dependent tables not already covered by explicit rules.
   Returns the full rule list (explicit + derived), preserving explicit order."
  [catalog explicit-rules]
  (let [explicit-tables (set (map :table explicit-rules))
        derived         (remove #(contains? explicit-tables (:table %))
                                (mapcat #(when (= :distributed (:type %))
                                           (compute-derived-rules catalog %))
                                        explicit-rules))]
    (into (vec explicit-rules) derived)))

(defn augment-catalog
  "For each distribute rule, augment the matching catalog entry by:
     1. Expanding explicit rules with FK-derived implicit rules (FK auto-discovery).
     2. Checking whether the distribution key column already exists.
     3. If not: copying the column definition from the last FROM table.
     4. Prepending the distribution key to :primary-key.
     5. Storing a :citus-read-sql override SELECT (with JOIN) on the entry.
   Returns the augmented catalog vector. Throws if FK chain is broken."
  [catalog distribute-rules & {:keys [source-type] :or {source-type :mysql}}]
  (let [all-rules   (expand-distribute-rules catalog distribute-rules)
        _           (when (> (count all-rules) (count distribute-rules))
                      (log/info (str "Citus FK auto-discovery: derived "
                                     (- (count all-rules) (count distribute-rules))
                                     " implicit rule(s) from "
                                     (count distribute-rules) " explicit rule(s)")))
        table-index (into {} (map (juxt :table-name identity) catalog))]
    (reduce
     (fn [cat rule]
       (if (= :reference (:type rule))
         cat
         (let [{:keys [table using from type]} rule
               from-chain (vec from)
               entry   (or (get table-index table)
                           (throw (ex-info (str "Citus: table not found in catalog: " table)
                                           {:rule rule})))
               col-exists? (some #(= using (:column-name %)) (:columns entry))]
           (if col-exists?
              ;; Column exists — just update primary key, no read override needed
             (mapv (fn [e]
                     (if (= table (:table-name e))
                       (update e :primary-key prepend-if-absent using)
                       e))
                   cat)
              ;; Column does not exist — find it in the last FROM table
             (let [from-table-name (last from-chain)
                   from-entry (or (get table-index from-table-name)
                                  (throw (ex-info
                                          (str "Citus: FROM table not found in catalog: "
                                               from-table-name)
                                          {:rule rule})))
                   dist-col-def (or (some #(when (= using (:column-name %)) %) (:columns from-entry))
                                    (throw (ex-info
                                            (str "Citus: distribution column '" using
                                                 "' not found in FROM table " from-table-name)
                                            {:rule rule})))
                   _ (check-fkeys-present catalog from-chain table rule table-index)
                   join-sql  (build-join-clause entry from-chain table-index)
                   read-sql  (build-backfill-select entry using from-table-name join-sql source-type)]
               (mapv (fn [e]
                       (if (= table (:table-name e))
                         (-> e
                             (update :columns #(vec (cons dist-col-def %)))
                             (update :primary-key prepend-if-absent using)
                             (assoc :citus-read-sql read-sql))
                         e))
                     cat))))))
     catalog
     all-rules)))

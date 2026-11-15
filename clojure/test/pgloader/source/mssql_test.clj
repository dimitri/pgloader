(ns pgloader.source.mssql-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.source.mssql :as mssql]
            [pgloader.source.protocol :as proto]))

(defn- sanitize [default pg-type]
  (#'mssql/sanitize-default default pg-type))

(deftest test-sanitize-default-string-literals
  (testing "N'…' Unicode literals and '…' literals yield their value"
    (is (= "new" (sanitize "N'new'" "text")))
    (is (= "new" (sanitize "'new'" "text")))
    (is (= "it's" (sanitize "N'it''s'" "text")))
    (is (= "" (sanitize "N''" "text"))))

  (testing "empty string literal on a numeric column is dropped (#1163)"
    (is (nil? (sanitize "''" "integer")))
    (is (nil? (sanitize "N''" "integer")))))

(deftest test-sanitize-default-tsql-functions
  (doseq [f ["SYSUTCDATETIME()" "sysutcdatetime()" "SYSDATETIME()" "GETDATE()"
             "getutcdate()" "sysdatetimeoffset()" "CURRENT_TIMESTAMP"]]
    (is (= "CURRENT_TIMESTAMP" (sanitize f "timestamptz")) f))
  (is (= "gen_random_uuid()" (sanitize "NEWID()" "uuid")))
  (is (= "gen_random_uuid()" (sanitize "newsequentialid()" "uuid"))))

(deftest test-sanitize-default-passthrough
  (is (= "0" (sanitize "0" "integer")))
  (is (= "nextval('public.order_seq')" (sanitize "NEXT VALUE FOR [dbo].[order_seq]" "bigint")))
  (is (nil? (sanitize "convert(datetime,'1753-01-01',0)" "timestamptz")))
  (is (nil? (sanitize nil "text"))))

(deftest test-catalog-prefilters-tables
  (testing "INCLUDING/EXCLUDING is applied before the per-table metadata queries"
    (let [queried (atom #{})]
      (with-redefs [mssql/connection           (fn [_] nil)
                    mssql/tables               (fn [_]
                                                 [{:table_schema "dbo" :table_name "keep_orders"}
                                                  {:table_schema "dbo" :table_name "keep_items"}
                                                  {:table_schema "dbo" :table_name "drop_audit"}])
                    mssql/table-columns        (fn [_ {:keys [table]}]
                                                 (swap! queried conj table)
                                                 [])
                    mssql/table-pkeys          (fn [_ _] [])
                    mssql/table-indexes        (fn [_ _] [])
                    mssql/table-fkeys          (fn [_ _] [])
                    mssql/table-extended-props (fn [_ _] [])]
        (let [src (mssql/create-source {:raw "mssql://sa@localhost/db"}
                                       {:table-filter #(re-find #"keep.*" %)})
              cat (proto/catalog src)]
          (is (= ["keep_items" "keep_orders"] (mapv :table-name cat)))
          (is (= #{"keep_items" "keep_orders"} @queried)
              "no metadata query for filtered-out tables"))))))

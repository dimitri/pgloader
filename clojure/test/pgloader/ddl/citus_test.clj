(ns pgloader.ddl.citus-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.ddl.citus :as citus]))

(set! *warn-on-reflection* true)

(def ^:private sample-catalog
  [{:table-name "billers"
    :schema "public"
    :columns [{:column-name "id"   :column-type "integer" :is-nullable false}
              {:column-name "name" :column-type "text"    :is-nullable false}]
    :primary-key ["id"]
    :indexes [] :fkeys []}

   {:table-name "receivable_accounts"
    :schema "public"
    :columns [{:column-name "id"        :column-type "integer" :is-nullable false}
              {:column-name "biller_id" :column-type "integer" :is-nullable false}]
    :primary-key ["id"]
    :indexes []
    :fkeys [{:name "fk_ra_biller" :columns ["biller_id"]
             :ftable "billers" :fcols ["id"]
             :on-delete "NO ACTION" :on-update "NO ACTION"}]}

   {:table-name "splits"
    :schema "public"
    :columns [{:column-name "id"                    :column-type "integer" :is-nullable false}
              {:column-name "receivable_account_id" :column-type "integer" :is-nullable false}
              {:column-name "amount"                :column-type "numeric" :is-nullable true}]
    :primary-key ["id"]
    :indexes []
    :fkeys [{:name "fk_splits_ra" :columns ["receivable_account_id"]
             :ftable "receivable_accounts" :fcols ["id"]
             :on-delete "NO ACTION" :on-update "NO ACTION"}]}])

(deftest test-augment-catalog-reference-passthrough
  (testing "Reference table rules do not modify the catalog entry"
    (let [rules [{:type :reference :table "billers"}]
          aug   (citus/augment-catalog sample-catalog rules)]
      (is (= (first sample-catalog) (first aug))))))

(deftest test-augment-catalog-distributed-col-exists
  (testing "DISTRIBUTE USING column that already exists: only pk updated"
    (let [rules [{:type :distributed :table "receivable_accounts" :using "biller_id"}]
          aug   (citus/augment-catalog sample-catalog rules)
          entry (first (filter #(= "receivable_accounts" (:table-name %)) aug))]
      ;; Column count unchanged
      (is (= 2 (count (:columns entry))))
      ;; biller_id prepended to primary key
      (is (= "biller_id" (first (:primary-key entry)))))))

(deftest test-augment-catalog-distributed-from-adds-column
  (testing "DISTRIBUTE USING col FROM t adds missing column to :columns"
    (let [rules [{:type :distributed-from :table "splits"
                  :using "biller_id" :from ["receivable_accounts"]}]
          aug   (citus/augment-catalog sample-catalog rules)
          entry (first (filter #(= "splits" (:table-name %)) aug))]
      ;; biller_id column prepended
      (is (= "biller_id" (:column-name (first (:columns entry)))))
      ;; Original columns still present
      (is (= 4 (count (:columns entry))))
      ;; biller_id prepended to primary key
      (is (= "biller_id" (first (:primary-key entry)))))))

(deftest test-augment-catalog-distributed-from-read-sql
  (testing "DISTRIBUTE USING col FROM t stores a JOIN SELECT in :citus-read-sql"
    (let [rules [{:type :distributed-from :table "splits"
                  :using "biller_id" :from ["receivable_accounts"]}]
          aug   (citus/augment-catalog sample-catalog rules)
          entry (first (filter #(= "splits" (:table-name %)) aug))
          sql   (:citus-read-sql entry)]
      (is (string? sql))
      (is (re-find #"receivable_accounts\.biller_id" sql))
      (is (re-find #"FROM.*splits" sql))
      (is (re-find #"JOIN.*receivable_accounts" sql))
      (is (re-find #"splits\.receivable_account_id\s*=\s*receivable_accounts\.id" sql)))))

(deftest test-augment-catalog-missing-fkey-throws
  (testing "Missing FK between table and FROM table throws a clear error"
    (let [broken-catalog (mapv (fn [e]
                                 (if (= "splits" (:table-name e))
                                   (assoc e :fkeys [])
                                   e))
                               sample-catalog)
          rules [{:type :distributed-from :table "splits"
                  :using "biller_id" :from ["receivable_accounts"]}]]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"No FK"
                            (citus/augment-catalog broken-catalog rules))))))

(deftest test-augment-catalog-pg-source-type
  (testing "PG source type wraps columns in CAST(... AS text) in the read SQL"
    (let [rules [{:type :distributed-from :table "splits"
                  :using "biller_id" :from ["receivable_accounts"]}]
          aug   (citus/augment-catalog sample-catalog rules :source-type :pgsql)
          entry (first (filter #(= "splits" (:table-name %)) aug))
          sql   (:citus-read-sql entry)]
      (is (string? sql))
      (is (re-find #"CAST\(" sql))
      (is (re-find #"AS text\)" sql))
      (is (not (re-find #"::text" sql))))))

(deftest test-augment-catalog-from-chain
  (testing "Two-hop FROM chain traverses correctly"
    (let [two-hop-catalog (conj sample-catalog
                                {:table-name "deep_table"
                                 :schema "public"
                                 :columns [{:column-name "id" :column-type "integer" :is-nullable false}
                                           {:column-name "split_id" :column-type "integer" :is-nullable false}]
                                 :primary-key ["id"]
                                 :indexes []
                                 :fkeys [{:name "fk_deep_split" :columns ["split_id"]
                                          :ftable "splits" :fcols ["id"]
                                          :on-delete "NO ACTION" :on-update "NO ACTION"}]})
          rules [{:type :distributed-from :table "deep_table"
                  :using "biller_id" :from ["splits" "receivable_accounts"]}]
          ;; This should fail because deep_table's FK goes to splits,
          ;; and splits has an FK to receivable_accounts, but there's
          ;; no FK from receivable_accounts to... wait, receivable_accounts
          ;; has an FK to billers, not to something that has biller_id.
          ;; The chain is: deep_table -> splits -> receivable_accounts
          ;; deep_table has FK to splits ✓
          ;; splits has FK to receivable_accounts ✓
          ;; Does receivable_accounts have biller_id? Yes ✓
          aug   (citus/augment-catalog two-hop-catalog rules)
          entry (first (filter #(= "deep_table" (:table-name %)) aug))
          sql   (:citus-read-sql entry)]
      (is (string? sql))
      (is (re-find #"receivable_accounts\.biller_id" sql))
      (is (re-find #"JOIN.*splits" sql))
      (is (re-find #"JOIN.*receivable_accounts" sql)))))

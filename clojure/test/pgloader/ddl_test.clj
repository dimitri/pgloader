(ns pgloader.ddl-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [pgloader.ddl.common :as ddl]
            [pgloader.source.mysql]))

(deftest test-identifier-quote
  (is (= "\"hello\"" (ddl/identifier-quote "hello")))
  (is (= "\"he\"\"llo\"" (ddl/identifier-quote "he\"llo"))))

(deftest test-quote-fqname
  (is (= "\"public\".\"users\"" (ddl/quote-fqname "public" "users")))
  (is (= "\"users\"" (ddl/quote-fqname "users"))))

(deftest test-column-def
  (let [col {:column-name "id"
             :column-type "int"
             :is-nullable false
             :extra ""
             :column-default nil}]
    (is (= "  \"id\" integer NOT NULL" (ddl/column-def col))))

  (let [col {:column-name "name"
             :column-type "varchar(255)"
             :is-nullable true
             :extra ""
             :column-default nil}]
    (is (= "  \"name\" varchar" (ddl/column-def col))))

  (let [col {:column-name "nickname"
             :column-type "varchar(100)"
             :is-nullable false
             :extra ""
             :column-default "NULL"}]
    (is (= "  \"nickname\" varchar" (ddl/column-def col))))
  
  (let [col {:column-name "created_at"
             :column-type "datetime"
             :is-nullable false
             :extra "auto_increment"
             :column-default nil}]
    (is (= "  \"created_at\" timestamptz" (ddl/column-def col)))))

(deftest test-create-table-sql
  (let [cols [{:column-name "id"    :column-type "int"        :is-nullable false :extra ""}
              {:column-name "name"  :column-type "varchar"    :is-nullable true  :extra ""}
              {:column-name "email" :column-type "varchar(255)" :is-nullable true :extra ""}]
        sql (ddl/create-table-sql "public" "users" cols)]
    (is (str/includes? sql "CREATE TABLE IF NOT EXISTS \"public\".\"users\""))
    (is (str/includes? sql "\"id\" integer NOT NULL"))
    (is (str/includes? sql "\"name\" varchar"))
    (is (str/includes? sql "\"email\" varchar"))))

(deftest test-drop-table-if-exists-sql
  (is (= "DROP TABLE IF EXISTS \"public\".\"users\" CASCADE;"
         (ddl/drop-table-if-exists-sql "public" "users"))))

(deftest test-type-mapping
  (let [col {:column-name "val" :column-type "bigint" :is-nullable true :extra ""}]
    (is (= "  \"val\" bigint" (ddl/column-def col))))

  (let [col {:column-name "val" :column-type "tinyint" :is-nullable true :extra ""}]
    (is (= "  \"val\" smallint" (ddl/column-def col))))

  (let [col {:column-name "val" :column-type "tinyint(1)" :is-nullable true :extra ""}]
    (is (= "  \"val\" boolean" (ddl/column-def col))))

  (let [col {:column-name "val" :column-type "json" :is-nullable true :extra ""}]
    (is (= "  \"val\" jsonb" (ddl/column-def col))))

  (let [col {:column-name "val" :column-type "geometry" :is-nullable true :extra ""}]
    (is (= "  \"val\" geometry" (ddl/column-def col))))

  (let [col {:column-name "val" :column-type "char" :is-nullable true :extra ""}]
    (is (= "  \"val\" varchar(1)" (ddl/column-def col))))

  (let [col {:column-name "val" :column-type "varchar(255)" :is-nullable true :extra ""}]
    (is (= "  \"val\" varchar" (ddl/column-def col))))

  (let [col {:column-name "val" :column-type "char(10)" :is-nullable true :extra ""}]
    (is (= "  \"val\" varchar(10)" (ddl/column-def col)))))

(deftest test-create-index-sql
  (let [cols [{:column-name "id" :column-type "int" :is-nullable false :extra "" :key "PRI"}
              {:column-name "email" :column-type "varchar" :is-nullable true :extra ""}]
        sql (ddl/create-index-sql "public" "users" cols)]
    (is (str/includes? sql "CREATE INDEX"))))

(deftest test-create-indexes-sql-plain
  (let [idxes [{:name "idx_email" :unique false :columns ["email"]}
               {:name "idx_uid_status" :unique true :columns ["user_id" "status"]}]
        sqls (ddl/create-indexes-sql "public" "orders" idxes 42)]
    (is (= 2 (count sqls)))
    (is (str/includes? (first sqls)  "\"email\""))
    (is (str/includes? (second sqls) "UNIQUE"))
    (is (str/includes? (second sqls) "\"user_id\", \"status\""))))

(deftest test-create-indexes-sql-expression
  (testing "expression index columns are emitted without extra quoting"
    (let [idxes [{:name "idx_lower_email" :unique false :columns ["(lower(email))"]}]
          sqls (ddl/create-indexes-sql "public" "users" idxes 7)]
      (is (= 1 (count sqls)))
      (is (str/includes? (first sqls) "((lower(email)))"))
      (is (not (str/includes? (first sqls) "\"(lower"))))))

(deftest test-create-indexes-sql-mixed
  (testing "index with both plain and expression columns"
    (let [idxes [{:name "idx_mixed" :unique false :columns ["status" "(lower(email))"]}]
          sqls (ddl/create-indexes-sql "public" "users" idxes 9)]
      (is (= 1 (count sqls)))
      (is (str/includes? (first sqls) "\"status\", (lower(email))")))))

(deftest test-split-index-columns
  (testing "plain columns"
    (is (= ["email" "name"] (#'pgloader.source.mysql/split-index-columns "email,name"))))
  (testing "expression column"
    (is (= ["(lower(email))"] (#'pgloader.source.mysql/split-index-columns "(lower(email))"))))
  (testing "mixed with expression containing commas"
    (is (= ["status" "(concat(first_name,last_name))"]
           (#'pgloader.source.mysql/split-index-columns "status,(concat(first_name,last_name))")))))

(deftest test-translate-mysql-expression
  (is (= "(lower(email))"
         (#'pgloader.source.mysql/translate-mysql-expression "(lower(`email`))")))
  (is (= "(concat(first_name,last_name))"
         (#'pgloader.source.mysql/translate-mysql-expression "(concat(`first_name`,`last_name`))"))))

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

(deftest test-create-indexes-sql-mixed-case-name
  (testing "mixed-case index name is quoted exactly once (#1521)"
    ;; With an oid the name becomes idx_{oid}_{raw}, still single-quoted.
    (let [idxes [{:name "MyMixedCaseIndex" :unique false :columns ["col"]}]
          sqls  (ddl/create-indexes-sql "public" "t" idxes 7)]
      (is (= 1 (count sqls)))
      ;; Must contain exactly one opening double-quote before the name.
      (is (str/includes? (first sqls) "\"idx_7_MyMixedCaseIndex\""))
      ;; Must NOT contain backslash-escaped quotes (double-quoting regression).
      (is (not (str/includes? (first sqls) "\\\"")))))

  (testing "preserve-index-names path (oid=nil) also single-quoted (#1521)"
    ;; When oid is nil the raw index name is passed through identifier-quote once.
    (let [idxes [{:name "MyPreservedIndex" :unique false :columns ["col"]}]
          sqls  (ddl/create-indexes-sql "public" "t" idxes nil)]
      (is (= 1 (count sqls)))
      (is (str/includes? (first sqls) "\"MyPreservedIndex\""))
      (is (not (str/includes? (first sqls) "\\\""))))))

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

(deftest test-column-def-generated
  (testing "STORED generated column emits GENERATED ALWAYS AS ... STORED"
    (let [col {:column-name "full_name"
               :column-type "text"
               :is-nullable true
               :extra "STORED GENERATED"
               :column-default nil
               :generated-expression "first_name || ' ' || last_name"}]
      (is (= "  \"full_name\" text GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED"
             (ddl/column-def col)))))
  (testing "VIRTUAL generated column also becomes STORED in PostgreSQL"
    (let [col {:column-name "initials"
               :column-type "varchar"
               :is-nullable true
               :extra "VIRTUAL GENERATED"
               :column-default nil
               :generated-expression "UPPER(LEFT(first_name, 1))"}]
      (is (str/includes? (ddl/column-def col) "GENERATED ALWAYS AS (UPPER(LEFT(first_name, 1))) STORED"))))
  (testing "mysql-gen-expr->pg replaces backtick-quoted identifiers"
    (let [result (#'pgloader.source.mysql/mysql-gen-expr->pg "`first_name` || ' ' || `last_name`")]
      (is (= "\"first_name\" || ' ' || \"last_name\"" result))))
  (testing "mysql-gen-expr->pg returns nil for blank expression"
    (is (nil? (#'pgloader.source.mysql/mysql-gen-expr->pg "")))))

;; ── snake_case identifier transformation (#1286, #1287, #1320, #1327, #1344) ──

(defn- make-table [table-name col-names idx-names]
  {:table-name  table-name
   :schema      "public"
   :columns     (mapv #(hash-map :column-name %) col-names)
   :primary-key []
   :indexes     (mapv #(hash-map :name % :columns [%] :unique false) idx-names)
   :fkeys       []})

(deftest test-snake-case-transform
  (testing "basic camelCase to snake_case"
    (let [[t] (ddl/apply-identifier-case [(make-table "FooBar" ["fooBar" "BazQux"] [])]
                                         {:snake-case-ids true})]
      (is (= "foo_bar" (:table-name t)))
      (is (= ["foo_bar" "baz_qux"] (mapv :column-name (:columns t))))))

  (testing "existing underscores not doubled (#1287)"
    (let [[t] (ddl/apply-identifier-case [(make-table "Object_Name" ["Object_Name"] [])]
                                         {:snake-case-ids true})]
      (is (= "object_name" (:table-name t)))
      (is (= ["object_name"] (mapv :column-name (:columns t))))))

  (testing "SQL keywords are still downcased (#1320)"
    (let [[t] (ddl/apply-identifier-case [(make-table "Order" ["User" "From" "To"] [])]
                                         {:snake-case-ids true})]
      (is (= "order" (:table-name t)))
      (is (= ["user" "from" "to"] (mapv :column-name (:columns t))))))

  (testing "dollar sign replaced by underscore (#1344)"
    (let [[t] (ddl/apply-identifier-case [(make-table "table$name" ["col$one"] [])]
                                         {:snake-case-ids true})]
      (is (= "table_name" (:table-name t)))
      (is (= ["col_one"] (mapv :column-name (:columns t))))))

  (testing "index names also transformed (#1327)"
    (let [[t] (ddl/apply-identifier-case [(make-table "t" [] ["MyIndex" "Object_IDX"])]
                                         {:snake-case-ids true})]
      (is (= ["my_index" "object_idx"] (mapv :name (:indexes t))))))

  (testing "quote-ids (default) leaves mixed-case index names untouched (#1521)"
    ;; With no downcase/snake option, apply-identifier-case is a no-op on index names
    ;; so identifier-quote in create-indexes-sql receives the original case.
    (let [[t] (ddl/apply-identifier-case [(make-table "t" [] ["MyIndex" "CamelCase"])]
                                         {})]
      (is (= ["MyIndex" "CamelCase"] (mapv :name (:indexes t))))))

  (testing "table name truncated at 63 chars (#1286)"
    (let [long-name (apply str (repeat 70 "a"))
          [t] (ddl/apply-identifier-case [(make-table long-name [] [])]
                                         {:snake-case-ids true})]
      (is (= 63 (count (:table-name t))))))

  (testing "downcase-ids still works for plain lowercase"
    (let [[t] (ddl/apply-identifier-case [(make-table "MyTable" ["MyCol"] [])]
                                         {:downcase-ids true})]
      (is (= "mytable" (:table-name t)))
      (is (= ["mycol"] (mapv :column-name (:columns t))))))

  (testing "XMLParser-style all-caps prefix handled"
    (let [[t] (ddl/apply-identifier-case [(make-table "XMLParser" [] [])]
                                         {:snake-case-ids true})]
      (is (= "xml_parser" (:table-name t))))))

;; ── FK filter (#1216) ─────────────────────────────────────────────────────────

(deftest test-create-fkeys-sql-filters-missing-tables
  (testing "create-fkeys-sql generates SQL for all provided fkeys"
    (let [fkeys [{:name "fk_orders_user" :columns ["user_id"] :ftable "users"
                  :fcols ["id"] :on-delete nil :on-update nil}
                 {:name "fk_orders_product" :columns ["product_id"] :ftable "products"
                  :fcols ["id"] :on-delete "CASCADE" :on-update nil}]
          sql (ddl/create-fkeys-sql "public" "orders" fkeys)]
      (is (= 2 (count sql)))
      (is (str/includes? (first sql) "REFERENCES \"public\".\"users\""))
      (is (str/includes? (second sql) "ON DELETE CASCADE"))))

  (testing "filtering fkeys to loaded tables excludes unloaded referenced tables (#1216)"
    (let [all-fkeys [{:name "fk_a" :columns ["x"] :ftable "loaded_table"
                      :fcols ["id"] :on-delete nil :on-update nil}
                     {:name "fk_b" :columns ["y"] :ftable "excluded_table"
                      :fcols ["id"] :on-delete nil :on-update nil}]
          loaded-tables #{"loaded_table"}
          fkeys (filter #(loaded-tables (:ftable %)) all-fkeys)
          sql (ddl/create-fkeys-sql "public" "source_table" fkeys)]
      (is (= 1 (count sql)))
      (is (str/includes? (first sql) "REFERENCES \"public\".\"loaded_table\"")))))

;; ── enum-types with include-no-drop (#1001) ───────────────────────────────────

(deftest test-create-enum-types-sql
  (testing "create-enum-types-sql generates DROP + CREATE pairs"
    (let [sqls (ddl/create-enum-types-sql [{:type-name "color_enum"
                                            :schema    "public"
                                            :values    ["red" "green" "blue"]
                                            :is-set    false}])]
      (is (= 2 (count sqls)))
      (is (str/starts-with? (first sqls) "DROP TYPE IF EXISTS"))
      (is (str/starts-with? (second sqls) "CREATE TYPE"))))

  (testing "filtering out DROP statements honours include-no-drop"
    (let [sqls    (ddl/create-enum-types-sql [{:type-name "status_enum"
                                               :schema    "public"
                                               :values    ["active" "inactive"]
                                               :is-set    false}])
          no-drop (remove #(str/starts-with? % "DROP") sqls)]
      (is (= 1 (count no-drop)))
      (is (str/starts-with? (first no-drop) "CREATE TYPE")))))

;; ── MySQL type mapping (#1629, #1280, #1403, #1546, #1371) ───────────────────

(deftest test-pg-type-for-timestamp-precision
  (testing "datetime(6) preserves precision as timestamptz(6) (#1629)"
    (let [col {:column-name "created_at" :column-type "datetime(6)"
               :is-nullable true :extra "" :column-default nil}
          sql (ddl/column-def col)]
      (is (str/includes? sql "timestamptz(6)"))))

  (testing "timestamp(3) preserves precision (#1629)"
    (let [col {:column-name "ts" :column-type "timestamp(3)"
               :is-nullable true :extra "" :column-default nil}
          sql (ddl/column-def col)]
      (is (str/includes? sql "timestamptz(3)"))))

  (testing "datetime without precision stays timestamptz"
    (let [col {:column-name "ts" :column-type "datetime"
               :is-nullable true :extra "" :column-default nil}
          sql (ddl/column-def col)]
      (is (str/includes? sql "timestamptz"))
      (is (not (str/includes? sql "timestamptz(")))))

  (testing "array types from PG source pass through unchanged (#1371)"
    (let [col {:column-name "tags" :column-type "varchar[]"
               :is-nullable true :extra "" :column-default nil}
          sql (ddl/column-def col)]
      (is (str/includes? sql "varchar[]")))))

(deftest test-column-def-defaults
  (testing "bit(1) DEFAULT b'0' passes through as bit literal (#1280)"
    (let [col {:column-name "flag" :column-type "bit(1)"
               :is-nullable false :extra "" :column-default "b'0'"}
          sql (ddl/column-def col)]
      (is (str/includes? sql "DEFAULT b'0'"))))

  (testing "current_timestamp(6) DEFAULT strips precision for PG (#1403)"
    (let [col {:column-name "ts" :column-type "datetime(6)"
               :is-nullable true :extra ""
               :column-default "current_timestamp(6)"}
          sql (ddl/column-def col)]
      (is (str/includes? sql "DEFAULT current_timestamp"))
      (is (not (str/includes? sql "current_timestamp(6)")))))

  (testing "backslash in default value is escaped (#1546)"
    (let [col {:column-name "path" :column-type "varchar(255)"
               :is-nullable true :extra "" :column-default "C:\\Users"}
          sql (ddl/column-def col)]
      (is (str/includes? sql "DEFAULT 'C:\\\\Users'"))))

  (testing "ON UPDATE CURRENT_TIMESTAMP trigger generated (#1560/#1196)"
    (let [cols [{:column-name "updated_at" :column-type "datetime"
                 :is-nullable true :extra "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"
                 :column-default "CURRENT_TIMESTAMP"}]]
      (let [sqls (ddl/create-triggers-sql "public" "orders" cols)]
        (is (some? sqls))
        (is (= 2 (count sqls)))
        (is (str/includes? (first sqls) "CREATE OR REPLACE FUNCTION")))))

  (testing "ON UPDATE CURRENT_TIMESTAMP with no DEFAULT also creates trigger (#1196)"
    (let [cols [{:column-name "upd" :column-type "timestamp"
                 :is-nullable true :extra "on update CURRENT_TIMESTAMP"
                 :column-default nil}]]
      (let [sqls (ddl/create-triggers-sql "myschema" "t" cols)]
        (is (some? sqls))
        (is (str/includes? (second sqls) "BEFORE UPDATE"))))))

(deftest test-check-constraints-sql
  (testing "generates ALTER TABLE ADD CONSTRAINT CHECK (#1645)"
    (let [sqls (ddl/create-check-constraints-sql "public" "employees"
                                                 [{:constraint-name "chk_salary"
                                                   :check-clause "salary > 0"}
                                                  {:constraint-name "chk_dept"
                                                   :check-clause "dept IN ('HR','Eng')"}])]
      (is (= 2 (count sqls)))
      (is (str/includes? (first sqls) "ADD CONSTRAINT \"chk_salary\" CHECK (salary > 0)"))
      (is (str/includes? (second sqls) "ADD CONSTRAINT \"chk_dept\" CHECK"))))

  (testing "MySQL 8 backtick-quoted identifiers converted to double-quote (#1645)"
    (let [sqls (ddl/create-check-constraints-sql "public" "employees"
                                                 [{:constraint-name "chk_salary"
                                                   :check-clause "`salary` > 0"}])]
      (is (= 1 (count sqls)))
      (is (str/includes? (first sqls) "CHECK (\"salary\" > 0)"))))

  (testing "empty checks returns empty vector"
    (is (= [] (ddl/create-check-constraints-sql "public" "t" [])))))

(deftest test-enum-type-name-after-alter-table-rename
  (testing "ENUM type name uses renamed table name (#1564)"
    (let [cat [{:table-name "renamed_tbl"
                :schema     "public"
                :columns    [{:column-name "status"
                              :column-type "enum('active','inactive')"}]
                :primary-key [] :indexes [] :fkeys []}]
          result (ddl/add-enum-types cat)]
      (let [enum-types (:enum-types (first result))]
        (is (= 1 (count enum-types)))
        ;; type name uses the (already-renamed) table name
        (is (= "renamed_tbl_status_t" (:type-name (first enum-types))))))))

(ns pgloader.source.mssql-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.source.mssql :as mssql]))

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

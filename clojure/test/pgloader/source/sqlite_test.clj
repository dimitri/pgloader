(ns pgloader.source.sqlite-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.source.sqlite :as sqlite]))

(set! *warn-on-reflection* true)

(deftest sqlite-type->pg-test
  (testing "standard text-affinity types"
    (is (= "text" (#'sqlite/sqlite-type->pg "text")))
    (is (= "text" (#'sqlite/sqlite-type->pg "varchar")))
    (is (= "text" (#'sqlite/sqlite-type->pg "char")))
    (is (= "text" (#'sqlite/sqlite-type->pg "clob"))))

  (testing "integer-affinity types — each sized alias is distinct"
    (is (= "integer"  (#'sqlite/sqlite-type->pg "int")))
    (is (= "bigint"   (#'sqlite/sqlite-type->pg "integer")))
    (is (= "smallint" (#'sqlite/sqlite-type->pg "tinyint")))
    (is (= "smallint" (#'sqlite/sqlite-type->pg "smallint")))
    (is (= "smallint" (#'sqlite/sqlite-type->pg "int2")))
    (is (= "integer"  (#'sqlite/sqlite-type->pg "int4")))
    (is (= "bigint"   (#'sqlite/sqlite-type->pg "int8")))
    (is (= "bigint"   (#'sqlite/sqlite-type->pg "bigint")))
    (is (= "bigint"   (#'sqlite/sqlite-type->pg "long"))))

  (testing "int4 and int8 are different types — not shadowed by 'int' prefix"
    (is (not= (#'sqlite/sqlite-type->pg "int4") (#'sqlite/sqlite-type->pg "int8")))
    (is (= "integer" (#'sqlite/sqlite-type->pg "int4")))
    (is (= "bigint"  (#'sqlite/sqlite-type->pg "int8"))))

  (testing "blob/binary types"
    (is (= "bytea" (#'sqlite/sqlite-type->pg "blob")))
    (is (= "bytea" (#'sqlite/sqlite-type->pg "BLOB"))))

  (testing "byte[] ORM array-blob types map to bytea (#1231)"
    (is (= "bytea" (#'sqlite/sqlite-type->pg "byte[]")))
    (is (= "bytea" (#'sqlite/sqlite-type->pg "BYTE[]")))
    (is (= "bytea" (#'sqlite/sqlite-type->pg "byte"))))

  (testing "real-affinity types"
    (is (= "double precision" (#'sqlite/sqlite-type->pg "real")))
    (is (= "double precision" (#'sqlite/sqlite-type->pg "float")))
    (is (= "real"             (#'sqlite/sqlite-type->pg "float4")))
    (is (= "double precision" (#'sqlite/sqlite-type->pg "float8")))
    (is (= "double precision" (#'sqlite/sqlite-type->pg "double")))
    (is (= "double precision" (#'sqlite/sqlite-type->pg "double precision")))
    (is (= "numeric"          (#'sqlite/sqlite-type->pg "decimal")))
    (is (= "numeric"          (#'sqlite/sqlite-type->pg "numeric"))))

  (testing "datetime types"
    (is (= "timestamptz" (#'sqlite/sqlite-type->pg "datetime")))
    (is (= "timestamptz" (#'sqlite/sqlite-type->pg "timestamp")))
    (is (= "date"        (#'sqlite/sqlite-type->pg "date")))
    (is (= "time"        (#'sqlite/sqlite-type->pg "time")))
    (is (= "boolean"     (#'sqlite/sqlite-type->pg "boolean"))))

  (testing "prefix matching for varchar(255), int(11) etc. still works"
    (is (= "text"    (#'sqlite/sqlite-type->pg "varchar(255)")))
    (is (= "integer" (#'sqlite/sqlite-type->pg "int(11)"))))

  (testing "unknown types fall through to text"
    (is (= "text" (#'sqlite/sqlite-type->pg "unknown_type")))))

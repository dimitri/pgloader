(ns pgloader.source.sqlite-test
  (:require [clojure.test :refer [deftest is testing]]
            [next.jdbc :as next.jdbc]
            [pgloader.source.sqlite :as sqlite]
            [pgloader.load-file.parser :as pgloader.load-file.parser]))

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

;; ---------------------------------------------------------------------------
;; catalog-views — shape and content (uses a real in-memory SQLite DB)
;; ---------------------------------------------------------------------------

(defn- in-memory-source []
  (let [conn (java.sql.DriverManager/getConnection "jdbc:sqlite::memory:")
        src  (sqlite/->SQLiteSource conn ":memory:" :downcase-ids)]
    [conn src]))

(deftest catalog-views-empty-test
  (testing "returns empty seq when no views exist"
    (let [[_ src] (in-memory-source)]
      (is (= [] (sqlite/catalog-views src))))))

(deftest catalog-views-basic-test
  (testing "catalog-views returns one entry per view"
    (let [[conn src] (in-memory-source)]
      (next.jdbc/execute! conn ["CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"])
      (next.jdbc/execute! conn ["CREATE VIEW named_people AS SELECT id, name FROM people WHERE name IS NOT NULL"])
      (let [views (sqlite/catalog-views src)]
        (is (= 1 (count views)))
        (let [v (first views)]
          (is (= "named_people" (:table-name v)))
          (is (= "named_people" (:source-table-name v)))
          (is (= "public" (:schema v)))
          (is (true? (:is-view v)))
          (is (= [] (:primary-key v)))
          (is (= [] (:indexes v)))
          (is (= [] (:fkeys v)))
          (is (= 2 (count (:columns v))))
          (is (= "id"   (:column-name (first (:columns v)))))
          (is (= "name" (:column-name (second (:columns v))))))))))

(deftest catalog-views-multiple-test
  (testing "catalog-views returns all views sorted by name"
    (let [[conn src] (in-memory-source)]
      (next.jdbc/execute! conn ["CREATE TABLE t (x INTEGER, y TEXT)"])
      (next.jdbc/execute! conn ["CREATE VIEW zview AS SELECT x FROM t"])
      (next.jdbc/execute! conn ["CREATE VIEW aview AS SELECT y FROM t"])
      (let [views (sqlite/catalog-views src)]
        (is (= 2 (count views)))
        (is (= "aview" (:table-name (first views))))
        (is (= "zview" (:table-name (second views))))))))

;; ---------------------------------------------------------------------------
;; Grammar: MATERIALIZE VIEWS clause accepted for SQLite load commands
;; ---------------------------------------------------------------------------

(deftest parser-materialize-all-views-test
  (testing "MATERIALIZE ALL VIEWS is accepted in a SQLite LOAD DATABASE command"
    (let [result (pgloader.load-file.parser/parse-string
                  "LOAD DATABASE FROM sqlite:///tmp/test.db
                   INTO postgresql://localhost/tgt
                   MATERIALIZE ALL VIEWS;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (is (= :all (:materialize-views (:ok result)))))))

(deftest parser-materialize-named-views-test
  (testing "MATERIALIZE VIEWS with names is accepted in a SQLite LOAD DATABASE command"
    (let [result (pgloader.load-file.parser/parse-string
                  "LOAD DATABASE FROM sqlite:///tmp/test.db
                   INTO postgresql://localhost/tgt
                   MATERIALIZE VIEWS my_view;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [mvs (:materialize-views (:ok result))]
        (is (sequential? mvs))
        (is (= 1 (count mvs)))
        (is (= "my_view" (:name (first mvs))))
        (is (nil? (:query (first mvs))))))))

(deftest parser-materialize-view-with-definition-test
  (testing "MATERIALIZE VIEWS with inline SQL definition is parsed correctly"
    (let [result (pgloader.load-file.parser/parse-string
                  "LOAD DATABASE FROM sqlite:///tmp/test.db
                   INTO postgresql://localhost/tgt
                   MATERIALIZE VIEWS renamed
                     AS $$ SELECT id, name AS product_name FROM products $$;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [mvs (:materialize-views (:ok result))]
        (is (sequential? mvs))
        (is (= 1 (count mvs)))
        (is (= "renamed" (:name (first mvs))))
        (is (= "SELECT id, name AS product_name FROM products" (:query (first mvs)))))))

  (testing "multiple views — mix of named-only and with-definition"
    (let [result (pgloader.load-file.parser/parse-string
                  "LOAD DATABASE FROM sqlite:///tmp/test.db
                   INTO postgresql://localhost/tgt
                   MATERIALIZE VIEWS existing_view,
                     custom AS $$ SELECT a, b AS alias FROM t $$;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [mvs (:materialize-views (:ok result))]
        (is (= 2 (count mvs)))
        (is (nil?    (:query (first mvs))))
        (is (string? (:query (second mvs))))))))

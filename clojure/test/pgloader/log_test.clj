(ns pgloader.log-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.log :as plog]))

(deftest test-redact-uri
  (testing "user:password@ in pgloader URIs"
    (is (= "postgresql://postgres:****@v4target/shopdb"
           (plog/redact-uri "postgresql://postgres:postgres@v4target/shopdb")))
    (is (= "mssql://pgloader_ro:****@mssql-source/shopdb"
           (plog/redact-uri "mssql://pgloader_ro:pgloader_RO_dev_only1@mssql-source/shopdb")))
    (is (= "mysql://root:****@mysql:3306/db?useSSL=false"
           (plog/redact-uri "mysql://root:p@ss:w0rd@mysql:3306/db?useSSL=false")))
    (is (= "jdbc:postgresql://u:****@host:5432/db"
           (plog/redact-uri "jdbc:postgresql://u:secret@host:5432/db"))))

  (testing "password= parameters in JDBC URLs"
    (is (= "jdbc:sqlserver://host:1433;databaseName=db;user=sa;password=****;encrypt=false"
           (plog/redact-uri "jdbc:sqlserver://host:1433;databaseName=db;user=sa;password=pgloaderTest1!;encrypt=false")))
    (is (= "jdbc:postgresql://host/db?user=u&Password=****&ssl=true"
           (plog/redact-uri "jdbc:postgresql://host/db?user=u&Password=secret&ssl=true"))))

  (testing "URIs without a password are unchanged"
    (is (= "postgresql://postgres@host/db" (plog/redact-uri "postgresql://postgres@host/db")))
    (is (= "postgresql://host:5432/db" (plog/redact-uri "postgresql://host:5432/db")))
    (is (= "mysql://shopdb/" (plog/redact-uri "mysql://shopdb/")))
    (is (nil? (plog/redact-uri nil)))))

(ns pgloader.load-file.ast-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.load-file.ast :refer [parse-uri]])
  (:import [java.sql DriverManager]))

(deftest test-parse-mysql-uri
  (testing "mysql:// with query params"
    (let [m (parse-uri "mysql://root:secret@localhost:3306/mydb?useSSL=false")]
      (is (= :mysql (:type m)))
      (is (= "localhost" (:host m)))
      (is (= 3306 (:port m)))
      (is (= "mydb" (:db m)))
      (is (= "root" (:user m)))
      (is (= "secret" (:password m)))
      (is (= "jdbc:mysql://localhost:3306/mydb?useSSL=false" (:jdbc-url m))
          "useSSL param preserved in jdbc-url"))))

(deftest test-parse-jdbc-mysql-uri
  (testing "jdbc:mysql:// passthrough"
    (let [m (parse-uri "jdbc:mysql://mysql:3306/sakila?useSSL=false&allowPublicKeyRetrieval=true")]
      (is (= :mysql (:type m)))
      (is (= "mysql" (:host m)))
      (is (= 3306 (:port m)))
      (is (= "sakila" (:db m)))
      (is (= "jdbc:mysql://mysql:3306/sakila?useSSL=false&allowPublicKeyRetrieval=true"
             (:jdbc-url m))
          "full JDBC URL preserved"))))

(deftest test-parse-jdbc-postgresql-uri
  (testing "jdbc:postgresql:// passthrough"
    (let [m (parse-uri "jdbc:postgresql://pg:5432/mydb?sslmode=require")]
      (is (= :pgsql (:type m)))
      (is (= "pg" (:host m)))
      (is (= 5432 (:port m)))
      (is (= "mydb" (:db m)))
      (is (= "jdbc:postgresql://pg:5432/mydb?sslmode=require" (:jdbc-url m))))))

(deftest test-parse-jdbc-sqlserver-uri
  (testing "jdbc:sqlserver:// passed to driver as-is — no semicolon parsing"
    (let [url "jdbc:sqlserver://mssql:1433;databaseName=testdb;user=sa;password=P@ss!;encrypt=false"
          m   (parse-uri url)]
      (is (= :mssql (:type m)))
      (is (= url (:jdbc-url m)) "full JDBC URL preserved for direct driver use")
      ;; We do NOT extract host/port/db/user from JDBC URLs — the driver parses them
      (is (nil? (:user m))))))

(deftest test-parse-mssql-native-uri
  (testing "mssql:// synthesises jdbc-url"
    (let [m (parse-uri "mssql://sa:secret@mssql:1433/mydb")]
      (is (= :mssql (:type m)))
      (is (= "jdbc:sqlserver://mssql:1433;databaseName=mydb;encrypt=false"
             (:jdbc-url m))
          "mssql:// gets jdbc-url with databaseName and encrypt=false"))))

(deftest test-parse-postgresql-synthesises-jdbc-url
  (testing "postgresql:// synthesises jdbc-url"
    (let [m (parse-uri "postgresql://user:pass@pg:5432/mydb")]
      (is (= :pgsql (:type m)))
      (is (= "jdbc:postgresql://pg:5432/mydb" (:jdbc-url m)))))
  (testing "postgresql:// with sslmode query param preserved"
    (let [m (parse-uri "postgresql://pg:5432/mydb?sslmode=require")]
      (is (= "jdbc:postgresql://pg:5432/mydb?sslmode=require" (:jdbc-url m))))))

;; Driver-level URL validation — no live server needed.
;; DriverManager/getDriver calls Driver.acceptsURL() on each registered driver
;; and returns the matching one, or throws SQLException if none accepts the URL.
;; All three JDBC drivers are on the classpath and auto-register via ServiceLoader.

(defn- driver-accepts? [url]
  (try (DriverManager/getDriver url) true
       (catch java.sql.SQLException _ false)))

(deftest test-jdbc-urls-accepted-by-drivers
  (testing "mysql:// synthesised URL accepted by MySQL Connector/J"
    (is (driver-accepts? (:jdbc-url (parse-uri "mysql://root:pass@db:3306/mydb?useSSL=false")))))

  (testing "jdbc:mysql:// passthrough accepted by MySQL Connector/J"
    (is (driver-accepts? (:jdbc-url (parse-uri "jdbc:mysql://db:3306/mydb?useSSL=false&allowPublicKeyRetrieval=true")))))

  (testing "postgresql:// synthesised URL accepted by PG JDBC driver"
    (is (driver-accepts? (:jdbc-url (parse-uri "postgresql://pg:5432/mydb")))))

  (testing "postgresql:// with query params accepted by PG JDBC driver"
    (is (driver-accepts? (:jdbc-url (parse-uri "postgresql://pg:5432/mydb?sslmode=require")))))

  (testing "jdbc:postgresql:// passthrough accepted by PG JDBC driver"
    (is (driver-accepts? (:jdbc-url (parse-uri "jdbc:postgresql://pg:5432/mydb?sslmode=require")))))

  (testing "mssql:// synthesised URL accepted by MSSQL JDBC driver"
    (is (driver-accepts? (:jdbc-url (parse-uri "mssql://sa:pass@mssql:1433/mydb")))))

  (testing "jdbc:sqlserver:// passthrough accepted by MSSQL JDBC driver"
    (is (driver-accepts? (:jdbc-url (parse-uri "jdbc:sqlserver://mssql:1433;databaseName=testdb;user=sa;password=P@ss!;encrypt=false"))))))

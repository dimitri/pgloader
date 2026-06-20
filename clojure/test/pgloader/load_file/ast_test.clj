(ns pgloader.load-file.ast-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.load-file.ast :refer [parse-uri]]))

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
  (testing "jdbc:sqlserver:// with semicolon params"
    (let [m (parse-uri "jdbc:sqlserver://mssql:1433;databaseName=testdb;user=sa;password=P@ss!;encrypt=false")]
      (is (= :mssql (:type m)))
      (is (= "mssql" (:host m)))
      (is (= 1433 (:port m)))
      (is (= "testdb" (:db m)))
      (is (= "sa" (:user m)))
      (is (= "P@ss!" (:password m)))
      (is (= "jdbc:sqlserver://mssql:1433;databaseName=testdb;user=sa;password=P@ss!;encrypt=false"
             (:jdbc-url m))
          "full JDBC URL preserved for direct driver use"))))

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

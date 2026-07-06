(ns pgloader.load-file.ast-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [pgloader.load-file.ast :refer [parse-uri]]
            [pgloader.load-file.parser :as parser])
  (:import [java.sql DriverManager]))

(deftest test-cast-rule-when-not-null
  (testing "hiccup->cast-rule emits :when-not-null true when 'and not null' present (#1676)"
    (let [r (parser/parse-string
             "LOAD DATABASE FROM mysql://u@l/d INTO postgresql:///t
              CAST type datetime when default \"0000-00-00 00:00:00\" and not null
                   to timestamp drop not null drop default using zero-dates-to-null;")
          rule (first (:cast-rules (:ok r)))]
      (is (true? (:when-not-null rule)))
      (is (= "0000-00-00 00:00:00" (:when-default rule)))))
  (testing "no :when-not-null when 'and not null' absent"
    (let [r (parser/parse-string
             "LOAD DATABASE FROM mysql://u@l/d INTO postgresql:///t
              CAST type datetime when default \"0000-00-00 00:00:00\"
                   to timestamp drop default using zero-dates-to-null;")
          rule (first (:cast-rules (:ok r)))]
      (is (nil? (:when-not-null rule)))
      (is (= "0000-00-00 00:00:00" (:when-default rule))))))

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
  (testing "mssql:// synthesises jdbc-url with encrypt=false default"
    (let [m (parse-uri "mssql://sa:secret@mssql:1433/mydb")]
      (is (= :mssql (:type m)))
      (is (= "jdbc:sqlserver://mssql:1433;databaseName=mydb;encrypt=false"
             (:jdbc-url m))
          "mssql:// gets jdbc-url with databaseName and encrypt=false")))

  (testing "mssql:// with encrypt=true in query string — no duplicate encrypt=false"
    (let [m (parse-uri "mssql://sa:secret@mssql:1433/mydb?encrypt=true")]
      (is (= "jdbc:sqlserver://mssql:1433;databaseName=mydb;encrypt=true"
             (:jdbc-url m))
          "encrypt= in query string suppresses the default encrypt=false")))

  (testing "mssql:// with authentication= for Azure AD (non-interactive)"
    (let [m (parse-uri "mssql://user@server.database.windows.net:1433/mydb?authentication=ActiveDirectoryPassword&encrypt=true")]
      (is (= :mssql (:type m)))
      (is (str/includes? (:jdbc-url m) "authentication=ActiveDirectoryPassword"))
      (is (str/includes? (:jdbc-url m) "encrypt=true"))
      (is (not (str/includes? (:jdbc-url m) "encrypt=false"))
          "no duplicate encrypt=false when encrypt= already in query string"))))

(deftest test-msal4j-on-classpath
  (testing "MSAL4J is bundled — Azure AD non-interactive auth modes are available"
    (is (try (Class/forName "com.microsoft.aad.msal4j.PublicClientApplication")
             true
             (catch ClassNotFoundException _
               false))
        "com.microsoft.aad.msal4j must be on the classpath for ActiveDirectoryPassword / ActiveDirectoryServicePrincipal auth")))

(deftest test-azure-identity-on-classpath
  (testing "azure-identity is bundled — ActiveDirectoryDefault / AzCli / WorkloadIdentity auth modes are available"
    (is (try (Class/forName "com.azure.identity.DefaultAzureCredentialBuilder")
             true
             (catch ClassNotFoundException _
               false))
        "com.azure.identity must be on the classpath for ActiveDirectoryDefault / ActiveDirectoryAzCli / ActiveDirectoryWorkloadIdentity auth")))

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

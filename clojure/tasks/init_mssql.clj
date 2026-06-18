(ns init-mssql
  (:import [java.sql DriverManager]))

(let [url  "jdbc:sqlserver://localhost:1433;databaseName=master;encrypt=false"
      props (doto (java.util.Properties.)
              (.setProperty "user" "sa")
              (.setProperty "password" "pgloaderTest1!"))
      conn (DriverManager/getConnection url props)]
  (.setAutoCommit conn true)

  (let [s (.createStatement conn)]
    (.execute s "DROP DATABASE IF EXISTS testdb")
    (.execute s "CREATE DATABASE testdb")
    (.close s))
  (println "Created testdb database")
  (.close conn)

  (let [url2 "jdbc:sqlserver://localhost:1433;databaseName=testdb;encrypt=false"
        props2 (doto (java.util.Properties.)
                 (.setProperty "user" "sa")
                 (.setProperty "password" "pgloaderTest1!"))
        conn2 (DriverManager/getConnection url2 props2)
        s (.createStatement conn2)]
    (.execute s "CREATE TABLE customers (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100) NOT NULL, email NVARCHAR(200), created DATETIME2 DEFAULT GETDATE())")
    (println "Created customers table")
    (.execute s "CREATE TABLE orders (id INT IDENTITY(1,1) PRIMARY KEY, customer_id INT NOT NULL, amount DECIMAL(10,2), order_date DATE, CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(id))")
    (println "Created orders table")
    (.execute s "CREATE INDEX idx_orders_customer ON orders(customer_id)")
    (println "Created index")

    (.execute s "INSERT INTO customers (name, email) VALUES ('Alice', 'alice@example.com')")
    (.execute s "INSERT INTO customers (name, email) VALUES ('Bob', 'bob@example.com')")
    (.execute s "INSERT INTO customers (name, email) VALUES ('Charlie', NULL)")
    (println "Inserted 3 customers")
    (.execute s "INSERT INTO orders (customer_id, amount, order_date) VALUES (1, 99.99, '2024-01-15')")
    (.execute s "INSERT INTO orders (customer_id, amount, order_date) VALUES (1, 149.00, '2024-02-20')")
    (.execute s "INSERT INTO orders (customer_id, amount, order_date) VALUES (2, 49.50, '2024-03-01')")
    (println "Inserted 3 orders")

    (let [rs (.executeQuery s "SELECT count(*) AS n FROM customers")]
      (while (.next rs)
        (println "Customers count:" (.getInt rs "n"))))
    (let [rs (.executeQuery s "SELECT count(*) AS n FROM orders")]
      (while (.next rs)
        (println "Orders count:" (.getInt rs "n"))))
    (.close s)
    (.close conn2)))

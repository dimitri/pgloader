(ns pgloader.transforms-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.transforms :as t]))

(deftest test-sql-server-bit-to-boolean
  (is (= "false" (t/sql-server-bit-to-boolean "0")))
  (is (= "true"  (t/sql-server-bit-to-boolean "1")))
  (is (= "false" (t/sql-server-bit-to-boolean "((0))")))
  (is (= "true"  (t/sql-server-bit-to-boolean "((1))")))
  (is (= "false" (t/sql-server-bit-to-boolean 0)))
  (is (= "true"  (t/sql-server-bit-to-boolean 1)))
  (is (nil?      (t/sql-server-bit-to-boolean nil)))
  (is (nil?      (t/sql-server-bit-to-boolean "  "))))

(deftest test-bits-to-hex-bitstring
  (is (nil? (t/bits-to-hex-bitstring nil)))
  (is (= "0101" (t/bits-to-hex-bitstring "b'0101'")))
  (is (= "0"    (t/bits-to-hex-bitstring "b'0'")))
  (let [bs (String. (byte-array [(unchecked-byte 0xFF)]) "ISO-8859-1")]
    (is (= "X'ff'" (t/bits-to-hex-bitstring bs)))))

(deftest test-logical-to-boolean
  (is (nil?      (t/logical-to-boolean "?")))
  (is (nil?      (t/logical-to-boolean " ")))
  (is (nil?      (t/logical-to-boolean nil)))
  (is (= "true"  (t/logical-to-boolean "T")))
  (is (= "true"  (t/logical-to-boolean "t")))
  (is (= "true"  (t/logical-to-boolean "Y")))
  (is (= "true"  (t/logical-to-boolean "y")))
  (is (= "false" (t/logical-to-boolean "F")))
  (is (= "false" (t/logical-to-boolean "N"))))

(deftest test-db3-trim-string
  (is (nil? (t/db3-trim-string nil)))
  (is (= "hello" (t/db3-trim-string "hello   ")))
  (is (= "hello" (t/db3-trim-string "hello")))
  (is (= "" (t/db3-trim-string "   "))))

(deftest test-db3-numeric-to-pgsql-numeric
  (is (nil? (t/db3-numeric-to-pgsql-numeric nil)))
  (is (nil? (t/db3-numeric-to-pgsql-numeric "   ")))
  (is (= "3.14" (t/db3-numeric-to-pgsql-numeric "  3.14  ")))
  (is (= "42"   (t/db3-numeric-to-pgsql-numeric "42"))))

(deftest test-db3-numeric-to-pgsql-integer
  (is (nil? (t/db3-numeric-to-pgsql-integer nil)))
  (is (nil? (t/db3-numeric-to-pgsql-integer "   ")))
  (is (= "42"  (t/db3-numeric-to-pgsql-integer "  42  ")))
  (is (= "42"  (t/db3-numeric-to-pgsql-integer 42)))
  (is (nil?    (t/db3-numeric-to-pgsql-integer "abc"))))

(deftest test-db3-date-to-pgsql-date
  (is (nil? (t/db3-date-to-pgsql-date nil)))
  (is (nil? (t/db3-date-to-pgsql-date "")))
  (is (nil? (t/db3-date-to-pgsql-date "2024")))
  (is (= "2024-01-15" (t/db3-date-to-pgsql-date "20240115")))
  (is (= "1999-12-31" (t/db3-date-to-pgsql-date "19991231"))))

(deftest test-ensure-parse-integer
  (is (nil? (t/ensure-parse-integer nil)))
  (is (nil? (t/ensure-parse-integer "abc")))
  (is (nil? (t/ensure-parse-integer "1.5")))
  (is (= 42  (t/ensure-parse-integer 42)))
  (is (= 42  (t/ensure-parse-integer "42")))
  (is (= -7  (t/ensure-parse-integer " -7 "))))

(deftest test-convert-mysql-linestring
  (is (nil? (t/convert-mysql-linestring nil)))
  (is (nil? (t/convert-mysql-linestring "")))
  (is (= "[(-87.87342467651445,45.79684462673078),(-87.87170806274479,45.802110434248966)]"
         (t/convert-mysql-linestring
          "LINESTRING(-87.87342467651445 45.79684462673078,-87.87170806274479 45.802110434248966)"))))

(deftest test-varbinary-to-inet
  (testing "nil and empty input"
    (is (nil? (t/varbinary-to-inet nil)))
    (is (nil? (t/varbinary-to-inet "X")))
    (is (nil? (t/varbinary-to-inet ""))))
  (testing "IPv4 addresses (4-byte varbinary)"
    (is (= "127.255.255.255" (t/varbinary-to-inet "X7fffffff")))
    (is (= "81.95.238.207"   (t/varbinary-to-inet "X515feecf")))
    (is (= "0.0.0.0"         (t/varbinary-to-inet "X00000000")))
    (is (= "255.255.255.255" (t/varbinary-to-inet "Xffffffff"))))
  (testing "IPv6 address (16-byte varbinary)"
    (is (= "2001:7c0:710:c143:d167:6b49:d48c:2494"
           (t/varbinary-to-inet "X200107c00710c143d1676b49d48c2494")))))

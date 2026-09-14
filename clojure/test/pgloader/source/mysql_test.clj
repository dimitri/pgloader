(ns pgloader.source.mysql-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.source.mysql :as mysql])
  (:import [java.nio.charset StandardCharsets]))

(deftest test-decoding-charset
  (is (= "UTF-8" (.name (#'mysql/decoding-charset "utf8"))))
  (is (= "UTF-8" (.name (#'mysql/decoding-charset "UTF8MB4"))))
  (is (= "windows-1252" (.name (#'mysql/decoding-charset "latin1"))))
  (is (= "ISO-8859-9" (.name (#'mysql/decoding-charset "latin5"))))
  (is (= "UTF-8" (.name (#'mysql/decoding-charset "utf-8"))))
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unsupported charset"
                        (#'mysql/decoding-charset "no-such-charset"))))

(deftest test-mysql-select-sql-raw-text
  (let [cols [{:column-name "id"    :column-type "int"}
              {:column-name "name"  :column-type "varchar(100)"}
              {:column-name "notes" :column-type "text"}
              {:column-name "tier"  :column-type "\"shopdb\".\"t_tier_t\""
               :source-column-type "enum('a','b')"}
              {:column-name "geo"   :column-type "point"}]]
    (testing "without decoding, columns are selected as-is"
      (is (= "SELECT `id`, `name`, `notes`, `tier`, ST_AsText(`geo`) AS `geo` FROM `t`"
             (#'mysql/mysql-select-sql cols "t"))))
    (testing "with decoding, text columns are fetched as raw bytes"
      (is (= (str "SELECT `id`, CAST(`name` AS BINARY) AS `name`, "
                  "CAST(`notes` AS BINARY) AS `notes`, CAST(`tier` AS BINARY) AS `tier`, "
                  "ST_AsText(`geo`) AS `geo` FROM `t`")
             (#'mysql/mysql-select-sql cols "t" true))))))

(deftest test-convert-mysql-value-decoding
  (let [utf8-bytes (.getBytes "Jean-François Ekström" StandardCharsets/UTF_8)
        convert    #'mysql/convert-mysql-value]
    (testing "raw bytes of a text column are decoded with the DECODING charset"
      (is (= "Jean-François Ekström"
             (convert utf8-bytes "LONGBLOB" {:column-type "varchar(100)"} nil 1
                      StandardCharsets/UTF_8))))
    (testing "the same bytes read as latin1 are mojibake — what the bug produced"
      (is (= "Jean-FranÃ§ois EkstrÃ¶m"
             (convert utf8-bytes "LONGBLOB" {:column-type "varchar(100)"} nil 1
                      (#'mysql/decoding-charset "latin1")))))
    (testing "SET columns are decoded then formatted as arrays"
      (is (= "{a,b}"
             (convert (.getBytes "a,b" StandardCharsets/UTF_8) "LONGBLOB"
                      {:column-type "set('a','b')"} nil 1 StandardCharsets/UTF_8))))
    (testing "binary columns are still hex-encoded"
      (is (= "Xdead"
             (convert (byte-array [(unchecked-byte 0xde) (unchecked-byte 0xad)]) "VARBINARY"
                      {:column-type "varbinary(2)"} nil 1 StandardCharsets/UTF_8))))))

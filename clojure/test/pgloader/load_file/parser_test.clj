(ns pgloader.load-file.parser-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [pgloader.load-file.parser :as parser]
            [pgloader.load-file.ast :as ast])
  (:import [pgloader.load_file.ast LoadCommand]))

(deftest test-parse-simple-csv
  (let [result (parser/parse-string
                 "LOAD CSV FROM '/data/sample.csv' INTO postgresql:///target;")]
    (is (:ok result))
    (let [cmd (:ok result)]
      (is (instance? LoadCommand cmd))
      (is (= :csv (:load-type cmd))))))

(deftest test-parse-csv-with-options
  (let [result (parser/parse-string
                 "LOAD CSV FROM '/data/sample.csv'
                  INTO postgresql://user@localhost/db
                  WITH skip header = 1,
                       fields terminated by ',',
                       fields optionally enclosed by '\"',
                       fields escaped by '\\\\',
                       encoding 'utf-8';")]
    (is (:ok result))
    (let [cmd (:ok result)]
      (is (= :csv (:load-type cmd)))
      (is (= "/data/sample.csv" (get-in cmd [:source :path])))
      (is (= 1 (get-in cmd [:with-options :skip-header]))))))

(deftest test-parse-csv-into-table
  (let [result (parser/parse-string
                 "LOAD CSV FROM '/data/users.csv'
                  INTO postgresql:///target INTO public.users
                  (id, name, email);")]
    (is (:ok result))
    (let [cmd (:ok result)]
      (is (= :csv (:load-type cmd)))
      (is (= "public" (get-in cmd [:with-options :target-schema]))))))

(deftest test-parse-mysql-database
  (let [result (parser/parse-string
                 "LOAD DATABASE FROM mysql://user@localhost/mydb
                  INTO postgresql:///target
                  WITH create tables, create indexes, include drop;")]
    (is (:ok result))
    (let [cmd (:ok result)]
      (is (= :database (:load-type cmd)))
      (is (:create-tables (:with-options cmd)))
      (is (:create-indexes (:with-options cmd)))
      (is (:include-drop (:with-options cmd))))))

(deftest test-parse-mysql-with-cast
  (let [result (parser/parse-string
                 "LOAD DATABASE FROM mysql://user@localhost/mydb
                  INTO postgresql:///target
                  WITH create tables, include drop
                  SET maintenance_work_mem to '128MB',
                      client_encoding to 'UTF8'
                  CAST type datetime to timestamptz drop default drop not null using zero-dates-to-null,
                       type tinyint to boolean drop typemod;")]
    (is (:ok result))
    (let [cmd (:ok result)]
      (is (= :database (:load-type cmd)))
      (is (seq (:set-parameters cmd)))
      (is (seq (:cast-rules cmd))))))

(deftest test-parse-invalid
  (let [result (parser/parse-string "LOAD BOGUS;")]
    (is (:error result))))

(deftest test-parse-file-not-found
  (is (:error (parser/parse-file "/nonexistent/file.load"))))

(deftest test-comments
  (let [result (parser/parse-string
                 "-- This is a comment
                  LOAD CSV FROM '/data/sample.csv'
                  INTO postgresql:///target;")]
    (is (:ok result))))

(deftest test-parse-csv-nullif
  (let [result (parser/parse-string
                 "LOAD CSV FROM '/data/nullif.csv'
                  INTO postgresql:///target INTO public.nullif
                  WITH null if '\\N';")]
    (is (:ok result))
    (let [cmd (:ok result)]
      (is (= "\\N" (get-in cmd [:with-options :nullif]))))))

(deftest test-parse-csv-keep-unquoted-blanks
  (let [result (parser/parse-string
                 "LOAD CSV FROM '/data/blanks.csv'
                  INTO postgresql:///target
                  WITH keep unquoted blanks;")]
    (is (:ok result))
    (let [cmd (:ok result)]
      (is (:keep-unquoted-blanks (:with-options cmd))))))

(deftest test-parse-csv-trim-unquoted-blanks
  (let [result (parser/parse-string
                 "LOAD CSV FROM '/data/blanks.csv'
                  INTO postgresql:///target
                  WITH trim unquoted blanks;")]
    (is (:ok result))
    (let [cmd (:ok result)]
      (is (:trim-unquoted-blanks (:with-options cmd))))))

(deftest test-parse-csv-create-no-tables
  (let [result (parser/parse-string
                 "LOAD CSV FROM '/data/sample.csv'
                  INTO postgresql:///target
                  WITH create no tables;")]
    (is (:ok result))
    (let [cmd (:ok result)]
      (is (false? (:create-tables (:with-options cmd)))))))

(deftest test-parse-csv-date-format
  (testing "parse the CL csv-parse-date.load file, verifying column formats are extracted"
    (let [load-str (slurp "test/pgloader/load_file/csv-parse-date.load")
          result   (parser/parse-string load-str)]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [cmd          (:ok result)
            source       (:source cmd)
            col-formats  (:column-formats source)
            cols         (:columns source)]
        (is (= :csv (:load-type cmd)))
        (is (= 2 (count col-formats)))
        (is (some #(= "MM-DD-YYYY HH24-MI-SS.US" (:date-format %)) col-formats))
        (is (some #(= "HH24:MI.SS" (:date-format %)) col-formats))
        (is (= "ts" (:name (first (filter #(= "MM-DD-YYYY HH24-MI-SS.US" (:date-format %)) col-formats)))))
        (is (= "hr" (:name (first (filter #(= "HH24:MI.SS" (:date-format %)) col-formats)))))
        ;; column list from source
        (is (= 3 (count cols)))
        (is (= ["row num" "ts" "hr"] cols))
        ;; inline data preserved
        (is (:inline source))
        (is (seq (:inline-data source)))
        ;; no null-if specs in this file
        (is (nil? (:column-nullifs source)))
        ;; verify the INLINE data contains test rows
        (is (str/includes? (:inline-data source) "10-02-1999 00-33-12.123456"))))))

(deftest test-distribute-reference-parse
  (testing "DISTRIBUTE AS REFERENCE TABLE is parsed correctly"
    (let [result (parser/parse-string
                   "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    DISTRIBUTE mytable AS REFERENCE TABLE;")]
      (is (:ok result))
      (let [cmd (:ok result)]
        (is (= [{:type :reference :table "mytable"}]
               (:distribute-rules cmd)))))))

(deftest test-distribute-using-parse
  (testing "DISTRIBUTE USING column is parsed correctly"
    (let [result (parser/parse-string
                   "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    DISTRIBUTE orders USING customer_id;")]
      (is (:ok result))
      (let [cmd (:ok result)]
        (is (= [{:type :distributed :table "orders" :using "customer_id"}]
               (:distribute-rules cmd)))))))

(deftest test-distribute-multiple-rules
  (testing "Multiple DISTRIBUTE clauses are all captured"
    (let [result (parser/parse-string
                   "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    DISTRIBUTE ref_table AS REFERENCE TABLE
                    DISTRIBUTE fact_table USING dim_id;")]
      (is (:ok result))
      (let [cmd (:ok result)]
        (is (= 2 (count (:distribute-rules cmd))))
        (is (= :reference (:type (first (:distribute-rules cmd)))))
        (is (= :distributed (:type (second (:distribute-rules cmd)))))))))

(deftest test-no-distribute-rules-default
  (testing "LoadCommand without DISTRIBUTE section has empty distribute-rules"
    (let [result (parser/parse-string
                   "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t;")]
      (is (:ok result))
      (let [cmd (:ok result)]
        (is (= [] (:distribute-rules cmd)))))))

(deftest test-decoding-as-regex-parse
  (testing "DECODING TABLE NAMES MATCHING ~/pattern/ AS charset is parsed"
    (let [result (parser/parse-string
                   "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    DECODING TABLE NAMES MATCHING ~/encoding/ AS utf-8;")]
      (is (:ok result))
      (let [cmd (:ok result)]
        (is (= 1 (count (:decoding-as cmd))))
        (let [rule (first (:decoding-as cmd))]
          (is (= "utf-8" (:encoding rule)))
          (is (= 1 (count (:patterns rule))))
          (is (= :regex (:type (first (:patterns rule)))))
          (is (= "encoding" (:value (first (:patterns rule))))))))))

(deftest test-decoding-as-multiple-patterns
  (testing "DECODING TABLE NAMES MATCHING with multiple patterns"
    (let [result (parser/parse-string
                   "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    DECODING TABLE NAMES MATCHING ~/messed/, ~/encoding/ AS utf8;")]
      (is (:ok result))
      (let [cmd (:ok result)]
        (is (= 1 (count (:decoding-as cmd))))
        (is (= 2 (count (:patterns (first (:decoding-as cmd))))))))))

(deftest test-with-drop-schema
  (testing "WITH drop schema sets :drop-schema in with-options"
    (let [result (parser/parse-string
                   "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    WITH drop schema;")]
      (is (:ok result))
      (is (true? (get-in result [:ok :with-options :drop-schema]))))))

(deftest test-with-reindex
  (testing "WITH reindex sets :reindex in with-options"
    (let [result (parser/parse-string
                   "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    WITH reindex;")]
      (is (:ok result))
      (is (true? (get-in result [:ok :with-options :reindex])))))
  (testing "WITH drop indexes also sets :reindex"
    (let [result (parser/parse-string
                   "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    WITH drop indexes;")]
      (is (:ok result))
      (is (true? (get-in result [:ok :with-options :reindex]))))))

(deftest test-with-preserve-index-names
  (testing "WITH preserve index names sets :preserve-index-names"
    (let [result (parser/parse-string
                   "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    WITH preserve index names;")]
      (is (:ok result))
      (is (true? (get-in result [:ok :with-options :preserve-index-names])))))
  (testing "WITH uniquify index names sets :uniquify-index-names"
    (let [result (parser/parse-string
                   "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    WITH uniquify index names;")]
      (is (:ok result))
      (is (true? (get-in result [:ok :with-options :uniquify-index-names]))))))

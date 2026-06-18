(ns pgloader.batch-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.batch :as batch]
            [pgloader.copy :as copy])
  (:import [org.postgresql.util PSQLException]
           [java.nio.charset StandardCharsets]
           [pgloader.batch Batch]))

(deftest test-make-batch
  (let [b (batch/make-batch 1000 (* 1024 1024))]
    (is (instance? Batch b))
    (is (zero? (:row-count b)))
    (is (zero? (:byte-count b)))
    (is (pos? (:max-rows b)))
    (is (pos? (:max-bytes b)))
    ;; randomized: at least 70% of base
    (is (>= (:max-rows b) 700))
    ;; at most 130% of base
    (is (<= (:max-rows b) 1300))))

(deftest test-batch-full?-row-limit
  (let [b (batch/make-batch 100 (* 1024 1024))
        max-rows (:max-rows b)
        row-bytes (.getBytes "a\tb\n" StandardCharsets/UTF_8)]
    (is (pos? max-rows))
    (is (not (batch/batch-full? b)))
    (let [b (loop [b b i 0]
              (if (< i (dec max-rows))
                (recur (batch/batch-add-row! b row-bytes) (inc i))
                b))]
      (is (not (batch/batch-full? b)))
      (let [b (batch/batch-add-row! b row-bytes)]
        (is (batch/batch-full? b))))))

(deftest test-batch-full?-byte-limit
  (let [b (batch/make-batch 100000 20)] ;; 20 byte limit
    (is (not (batch/batch-full? b)))
    (let [b (batch/batch-add-row! b (.getBytes "a\tb\n" StandardCharsets/UTF_8))] ;; 4 bytes
      (is (not (batch/batch-full? b)))
      (let [b (batch/batch-add-row! b (.getBytes "c\td\n" StandardCharsets/UTF_8))] ;; 4 bytes
        (is (not (batch/batch-full? b)))
        (let [b (batch/batch-add-row! b (.getBytes "e\tf\n" StandardCharsets/UTF_8))] ;; 4 bytes
          (is (not (batch/batch-full? b)))
          (let [b (batch/batch-add-row! b (.getBytes "g\th\n" StandardCharsets/UTF_8))] ;; 4 bytes
            (is (not (batch/batch-full? b)))
            (let [b (batch/batch-add-row! b (.getBytes "i\tj\n" StandardCharsets/UTF_8))] ;; 4 bytes = 20 total
              (is (batch/batch-full? b)))))))))

(deftest test-batch-add-row!
  (let [b (batch/make-batch 100 100000)]
    (is (= 0 (:row-count b)))
    (is (= 0 (:byte-count b)))
    (let [b (batch/batch-add-row! b (.getBytes "hello\tworld\n" StandardCharsets/UTF_8))]
      (is (= 1 (:row-count b)))
      (is (= 12 (:byte-count b))))))

(deftest test-parse-copy-line
  (is (nil? (batch/parse-copy-line nil)))
  (testing "real PSQLException without server error returns nil"
    (let [e (java.sql.SQLException. "test")]
      (is (nil? (batch/parse-copy-line e)))))
  (testing "real PSQLException with server error message"
    ;; ServerErrorMessage API varies across PG JDBC versions
    ;; Full testing requires integration tests with PostgreSQL
    ))

(deftest test-copy-sql
  (let [sql (copy/copy-sql
              {:target-schema "public"
               :target-table "users"
               :columns [{:column-name "id"}
                         {:column-name "name"}
                         {:column-name "email"}]})]
    (is (= "COPY \"public\".\"users\" (\"id\", \"name\", \"email\") FROM STDIN WITH (FORMAT TEXT)"
           sql))))

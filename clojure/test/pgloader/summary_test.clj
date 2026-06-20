(ns pgloader.summary-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [pgloader.log :as log]
            [pgloader.stats :as stats]
            [pgloader.summary :as summary])
  (:import [java.io StringWriter]))

(set! *warn-on-reflection* true)

(deftest test-fmt-duration
  (testing "nanoseconds formatting — always seconds with 3dp"
    (is (= "0.000s" (log/fmt-duration 0)))
    (is (= "0.000s" (log/fmt-duration 100)))
    (is (= "0.001s" (log/fmt-duration 1000000)))
    (is (= "0.999s" (log/fmt-duration 999000000)))
    (is (= "1.000s" (log/fmt-duration 1000000000)))
    (is (= "12.345s" (log/fmt-duration 12345000000)))
    (is (= "1m12.345s" (log/fmt-duration 72345000000)))
    (is (= "1d 00:00:00.000" (log/fmt-duration 86400000000000)))))

(deftest test-fmt-bytes
  (testing "byte formatting"
    (is (= "0 B" (log/fmt-bytes 0)))
    (is (= "512 B" (log/fmt-bytes 512)))
    (is (= "1.00 kB" (log/fmt-bytes 1024)))
    (is (= "1.00 MB" (log/fmt-bytes 1048576)))
    (is (= "1.00 GB" (log/fmt-bytes 1073741824)))
    (is (= "12.34 MB" (log/fmt-bytes (long (* 12.34 1048576)))))
    (is (= "123.45 kB" (log/fmt-bytes (long (* 123.45 1024)))))))

(deftest test-stats-lifecycle
  (testing "stats create, update, and clear"
    (stats/clear!)
    (is (empty? (stats/entries :data)))

    (stats/new-entry! :data "test_table")
    (is (= 1 (count (stats/entries :data))))
    (is (= "test_table" (:label (first (stats/entries :data)))))

    (stats/update-entry! :data "test_table" :rows 100 :errs 2 :bytes 1024 :total-nanos 5000000000)
    (let [e (first (stats/entries :data))]
      (is (= 100 (:rows e)))
      (is (= 2 (:errs e)))
      (is (= 1024 (:bytes e)))
      (is (= 5000000000 (:total-nanos e))))

    (stats/clear!)
    (is (empty? (stats/entries :data)))))

(deftest test-totals
  (testing "grand-totals aggregation"
    (stats/clear!)
    (stats/new-entry! :data "a")
    (stats/update-entry! :data "a" :rows 10 :errs 1 :bytes 100 :total-nanos 1000)
    (stats/new-entry! :data "b")
    (stats/update-entry! :data "b" :rows 20 :errs 2 :bytes 200 :total-nanos 2000)

    (let [g (stats/grand-totals)]
      (is (= 30 (:rows g)))
      (is (= 3 (:errs g)))
      (is (= 300 (:bytes g)))
      (is (= 3000 (:total-nanos g))))))

(deftest test-summary-render
  (testing "summary output contains expected sections"
    (stats/clear!)
    (stats/new-entry! :data "users")
    (stats/update-entry! :data "users" :rows 1000 :errs 0 :bytes 12345 :total-nanos 5000000000)
    (stats/new-entry! :data "orders")
    (stats/update-entry! :data "orders" :rows 500 :errs 0 :bytes 67890 :total-nanos 3000000000)

    (let [sw (StringWriter.)]
      (binding [*out* sw]
        (summary/print-summary false))
      (let [output (.toString sw)]
        (is (str/includes? output "Results:"))
        (is (str/includes? output "users"))
        (is (str/includes? output "orders"))
        ;; errors column: right-aligned 9-wide, so 0 appears as "        0"
        (is (str/includes? output "        0"))
        ;; grand-total row uses ✓ for zero errors
        (is (str/includes? output "        ✓"))
        (is (str/includes? output "Total import time"))
        ;; separator uses plain ASCII dashes, not Unicode
        (is (str/includes? output "--------"))
        (is (str/includes? output "table name"))))))

(deftest test-summary-verbose
  (testing "verbose mode adds read/write columns"
    (stats/clear!)
    (stats/new-entry! :data "test")
    (stats/update-entry! :data "test" :rows 100 :errs 0 :bytes 1024
                         :total-nanos 1000000000 :rs-nanos 200000000 :ws-nanos 300000000)
    (let [sw (StringWriter.)]
      (binding [*out* sw]
        (summary/print-summary true))
      (let [output (.toString sw)]
        (is (str/includes? output "read"))
        (is (str/includes? output "write"))))))

(deftest test-empty-stats
  (testing "no entries produces no summary crash"
    (stats/clear!)
    (let [sw (StringWriter.)]
      (binding [*out* sw]
        (summary/print-summary false))
      (let [output (.toString sw)]
        (is (str/includes? output "Results:"))))))

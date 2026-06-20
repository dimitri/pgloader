(ns pgloader.source.csv-date-format-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.source.csv :as csv]))

(set! *warn-on-reflection* true)

(defn- pattern-no-us [cl-fmt]
  (first (csv/translate-cl-date-format cl-fmt)))

(defn- has-us? [cl-fmt]
  (second (csv/translate-cl-date-format cl-fmt)))

(deftest test-translate-cl-date-format-basic
  (testing "basic CL format string translation"
    (is (= "yyyy-MM-dd HH:mm:ss" (pattern-no-us "YYYY-MM-DD HH24:MI:SS")))
    (is (= "yyyy" (pattern-no-us "YYYY")))
    (is (= "MM" (pattern-no-us "MM")))
    (is (= "dd" (pattern-no-us "DD")))
    (is (= "HH" (pattern-no-us "HH24")))
    (is (= "mm" (pattern-no-us "MI")))
    (is (= "ss" (pattern-no-us "SS")))))

(deftest test-translate-cl-date-format-microseconds
  (testing "microsecond translation"
    (is (= "" (pattern-no-us "US")))
    (is (has-us? "US"))
    ;; dot is removed because appendFraction handles decimal point automatically
    (is (= "yyyy-MM-dd HH:mm:ss" (pattern-no-us "YYYY-MM-DD HH24:MI:SS.US")))
    (is (has-us? "YYYY-MM-DD HH24:MI:SS.US"))))

(deftest test-translate-cl-date-format-time-only
  (testing "time-only format"
    (is (= "HH:mm.ss" (pattern-no-us "HH24:MI.SS")))
    (is (not (has-us? "HH24:MI.SS")))))

(deftest test-translate-cl-date-format-preserves-delimiters
  (testing "delimiter characters are preserved verbatim"
    (is (= "MM-dd-yyyy" (pattern-no-us "MM-DD-YYYY")))
    (is (= "yyyy/MM/dd" (pattern-no-us "YYYY/MM/DD")))
    (is (= "HH:mm:ss" (pattern-no-us "HH24:MI:SS")))))

(deftest test-apply-date-format-date-time
  (testing "date-time string with standard format"
    (let [result (csv/apply-date-format "10-02-1999 00-33-12"
                                        "MM-DD-YYYY HH24-MI-SS")]
      (is (= "1999-10-02 00:33:12.000000" result)) "expected ISO timestamp")))

(deftest test-apply-date-format-date-time-us
  (testing "date-time string with microseconds"
    (let [result (csv/apply-date-format "10-02-1999 00-33-12.123456"
                                        "MM-DD-YYYY HH24-MI-SS.US")]
      (is (= "1999-10-02 00:33:12.123456" result)))))

(deftest test-apply-date-format-date-time-us-3digit
  (testing "date-time string with 3-digit fractional seconds (right-padded to 6)"
    (let [result (csv/apply-date-format "10-02-2014 00-33-13.123"
                                        "MM-DD-YYYY HH24-MI-SS.US")]
      (is (= "2014-10-02 00:33:13.123000" result)))))

(deftest test-apply-date-format-date-time-us-4digit
  (testing "date-time string with 4-digit fractional seconds (right-padded to 6)"
    (let [result (csv/apply-date-format "10-02-2014 00-33-14.1234"
                                        "MM-DD-YYYY HH24-MI-SS.US")]
      (is (= "2014-10-02 00:33:14.123400" result)))))

(deftest test-apply-date-format-time-only
  (testing "time-only string"
    (let [result (csv/apply-date-format "00:05.02" "HH24:MI.SS")]
      (is (= "00:05:02" result)) "expected ISO time")))

(deftest test-apply-date-format-time-only-2
  (testing "time-only string with later time"
    (let [result (csv/apply-date-format "18:25.52" "HH24:MI.SS")]
      (is (= "18:25:52" result)))))

(deftest test-apply-date-format-time-only-3
  (testing "time-only string"
    (let [result (csv/apply-date-format "19:24.59" "HH24:MI.SS")]
      (is (= "19:24:59" result)))))

(deftest test-apply-date-format-all-four-rows
  (testing "all four rows from csv-parse-date.load"
    (let [fmt-date "MM-DD-YYYY HH24-MI-SS.US"
          fmt-time "HH24:MI.SS"
          rows [["10-02-1999 00-33-12.123456" "00:05.02"]
                ["10-02-2014 00-33-13.123"   "18:25.52"]
                ["10-02-2014 00-33-14.1234"  "13:14.15"]
                ["10-02-2018 19-24-59"       "19:24.59"]]
          expected [["1999-10-02 00:33:12.123456" "00:05:02"]
                    ["2014-10-02 00:33:13.123000" "18:25:52"]
                    ["2014-10-02 00:33:14.123400" "13:14:15"]
                    ["2018-10-02 19:24:59.000000" "19:24:59"]]]
      (doseq [[[raw-date raw-time] [ex-date ex-time]] (map vector rows expected)]
        (let [result-date (csv/apply-date-format raw-date fmt-date)
              result-time (csv/apply-date-format raw-time fmt-time)]
          (is (re-matches #"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}" result-date)
              (str "date " raw-date " -> " result-date))
          (is (= ex-date result-date) (str "date mismatch: " raw-date))
          (is (= ex-time result-time) (str "time mismatch: " raw-time)))))))

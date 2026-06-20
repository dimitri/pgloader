(ns pgloader.copy-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.copy :as copy]))

(set! *warn-on-reflection* true)

(deftest test-format-row-normal
  (testing "normal row with matching cast-specs"
    (let [row ["hello" "world" "42"]
          cast-specs [nil nil nil]]
      (is (re-matches #"hello\tworld\t42\n" (copy/format-row row cast-specs))))))

(deftest test-format-row-null-values
  (testing "nil values become \\N"
    (let [row ["hello" nil "42"]
          cast-specs [nil nil nil]]
      (is (re-matches #"hello\t\\N\t42\n" (copy/format-row row cast-specs))))))

(deftest test-format-row-too-few-columns
  (testing "shorter row — missing columns become \\N"
    (let [row ["hello" "world"]         ;; only 2 values for 3 columns
          cast-specs [nil nil nil]]
      (is (re-matches #"hello\tworld\t\\N\n" (copy/format-row row cast-specs))))))

(deftest test-format-row-too-many-columns
  (testing "longer row — extra columns ignored"
    (let [row ["hello" "world" "42" "extra1" "extra2"] ;; 5 values for 3 columns
          cast-specs [nil nil nil]]
      (is (re-matches #"hello\tworld\t42\n" (copy/format-row row cast-specs))))))

(deftest test-format-row-with-casts
  (testing "cast functions are applied per column"
    (let [row ["1" "hello" "0"]
          cast-specs [:tinyint-to-boolean nil :tinyint-to-boolean]]
      (is (re-matches #"t\thello\tf\n" (copy/format-row row cast-specs))))))

(deftest test-format-row-cast-with-null
  (testing "cast on nil value produces \\N"
    (let [row ["1" nil "0"]
          cast-specs [:tinyint-to-boolean nil :tinyint-to-boolean]]
      (is (re-matches #"t\t\\N\tf\n" (copy/format-row row cast-specs))))))

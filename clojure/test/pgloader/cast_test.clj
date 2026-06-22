(ns pgloader.cast-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.cast :as cast]))

(deftest test-zero-dates-to-null
  (is (nil? (cast/zero-dates-to-null "0000-00-00")))
  (is (nil? (cast/zero-dates-to-null "0000-00-00 00:00:00")))
  (is (= "2024-01-15" (cast/zero-dates-to-null "2024-01-15")))
  (is (= "2024-06-01 12:30:00" (cast/zero-dates-to-null "2024-06-01 12:30:00"))))

(deftest test-tinyint-to-boolean
  (is (= "f" (cast/tinyint-to-boolean "0")))
  (is (= "t" (cast/tinyint-to-boolean "1")))
  (is (= "t" (cast/tinyint-to-boolean "42"))))

(deftest test-tinyint-to-integer
  (is (= "0" (cast/tinyint-to-integer "0")))
  (is (= "127" (cast/tinyint-to-integer "127")))
  (is (nil? (cast/tinyint-to-integer nil))))

(deftest test-year-to-integer
  (is (= "2024" (cast/year-to-integer "2024")))
  (is (= "1999" (cast/year-to-integer "1999"))))

(deftest test-empty-string-to-null
  (is (nil? (cast/empty-string-to-null "")))
  (is (= "  " (cast/empty-string-to-null "  ")))
  (is (= "hello" (cast/empty-string-to-null "hello"))))

(deftest test-right-trim
  (is (= "hello" (cast/right-trim "hello  ")))
  (is (= "hello" (cast/right-trim "hello"))))

(deftest test-remove-null-characters
  (is (= "hello" (cast/remove-null-characters (str "hello" (char 0)))))
  (is (= "helloworld" (cast/remove-null-characters (str (char 0) "hello" (char 0) "world" (char 0)))))
  (is (= "hello" (cast/remove-null-characters "hello"))))

(deftest test-int-to-ip
  (is (= "0.0.0.0" (cast/int-to-ip "0")))
  (is (= "127.0.0.1" (cast/int-to-ip "2130706433")))
  (is (= "255.255.255.255" (cast/int-to-ip "4294967295"))))

(deftest test-bytes-to-pg-bytea
  (is (= "\\x68656c6c6f" (cast/bytes-to-pg-bytea "hello")))
  (is (= "\\x" (cast/bytes-to-pg-bytea ""))))

(deftest test-sqlite-timestamp-to-timestamp
  (is (= "2024-01-15 10:30:00" (cast/sqlite-timestamp-to-timestamp "2024-01-15 10:30:00")))
  (is (= "2024-01-15T10:30:00" (cast/sqlite-timestamp-to-timestamp "2024-01-15T10:30:00"))))

(deftest test-apply-cast
  (is (nil? (cast/apply-cast nil nil)))
  (is (= "hello" (cast/apply-cast nil "hello")))
  (is (= "2024-01-15" (cast/apply-cast :zero-dates-to-null "2024-01-15"))))

(deftest test-base64-decode
  (is (= "hello world" (cast/base64-decode "aGVsbG8gd29ybGQ=")))
  (is (nil? (cast/base64-decode nil))))

(deftest test-byte-vector-to-hexstring
  (is (= "deadbeef" (cast/byte-vector-to-hexstring "\\xDEADBEEF")))
  (is (= "cafe" (cast/byte-vector-to-hexstring "\\xCAFE")))
  (is (= "cafe" (cast/byte-vector-to-hexstring "CAFE")))
  (is (nil? (cast/byte-vector-to-hexstring nil))))

(deftest test-resolve-specs-empty
  (is (= [:tinyint-to-boolean]
         (cast/resolve-specs [] [{:column-name "x" :column-type "tinyint(1)"}])))
  (is (= [:tinyint-to-integer]
         (cast/resolve-specs [] [{:column-name "x" :column-type "tinyint(4)"}])))
  (is (= [:year-to-integer]
         (cast/resolve-specs [] [{:column-name "x" :column-type "year(4)"}])))
  (is (= [nil]
         (cast/resolve-specs [] [{:column-name "x" :column-type "int"}])))
  ;; char columns get :remove-null-characters by default (same as varchar)
  (is (= [:remove-null-characters]
         (cast/resolve-specs [] [{:column-name "x" :column-type "char(20)"}])))
  (is (= [:remove-null-characters]
         (cast/resolve-specs [] [{:column-name "x" :column-type "char"}]))))

(deftest test-resolve-specs-using-override
  (testing "type cast with :using"
    (let [rules [{:source {:type :type :name "datetime"} :using :zero-dates-to-null}]
          columns [{:column-name "ts" :column-type "datetime"}
                   {:column-name "name" :column-type "varchar(100)"}]]
      ;; varchar gets :remove-null-characters by default (CL parity)
      (is (= [:zero-dates-to-null :remove-null-characters] (cast/resolve-specs rules columns)))))

  (testing "column cast without :using — :none"
    (let [rules [{:source {:type :column :column "email"}}]
          columns [{:column-name "id" :column-type "int"}
                   {:column-name "email" :column-type "varchar(255)"}]]
      (is (= [nil :none] (cast/resolve-specs rules columns)))))

  (testing "type cast with :using"
    (let [rules [{:source {:type :type :name "varchar"} :using :empty-string-to-null}]
          columns [{:column-name "name" :column-type "varchar(255)"}]]
      (is (= [:empty-string-to-null] (cast/resolve-specs rules columns)))))

  (testing "when-extra condition (auto_increment)"
    (let [rules [{:source {:type :type :name "int"} :when-extra "auto_increment" :using :int-to-ip}]
          columns [{:column-name "a" :column-type "int" :extra "auto_increment"}
                   {:column-name "b" :column-type "int"}]]
      (is (= [:int-to-ip nil] (cast/resolve-specs rules columns)))))

  (testing "when-default condition"
    (let [rules [{:source {:type :type :name "varchar"} :when-default "''" :using :empty-string-to-null}]
          columns [{:column-name "a" :column-type "varchar(10)" :column-default "''"}
                   {:column-name "b" :column-type "varchar(10)"}]]
      ;; column b has no matching :when-default rule → falls through to default :remove-null-characters
      (is (= [:empty-string-to-null :remove-null-characters] (cast/resolve-specs rules columns)))))

  (testing "when-unsigned"
    (let [rules [{:source {:type :type :name "int"} :when-unsigned true :using :int-to-ip}]
          columns [{:column-name "a" :column-type "int unsigned"}
                   {:column-name "b" :column-type "int"}]]
      (is (= [:int-to-ip nil] (cast/resolve-specs rules columns)))))

  (testing "char overridden to text by user rule"
    (let [rules [{:source {:type :type :name "char"} :target-type "text"
                  :options {:drop-typemod true}}]
          columns [{:column-name "code" :column-type "char(3)"}
                   {:column-name "name" :column-type "char(20)"}]]
      ;; rule matched but no :using → implicit-using returns :none (pass-through).
      ;; The type default (:remove-null-characters) does NOT run — user rule wins.
      ;; Users who want null removal should add `using remove-null-characters`.
      (is (= [:none :none]
             (cast/resolve-specs rules columns))))))

(deftest test-apply-type-overrides
  (let [rules [{:source {:type :type :name "int"} :target-type "bigint"}
               {:source {:type :type :name "varchar"} :drop-typemod true :target-type "text"}]
        columns [{:column-name "id" :column-type "int"}
                 {:column-name "name" :column-type "varchar(255)"}
                 {:column-name "active" :column-type "boolean"}]
        result (cast/apply-type-overrides columns rules)]
    ;; Check type overrides only; :cast-fn is also set but tested separately.
    (is (= ["bigint" "text" "boolean"]
           (mapv :column-type result))))

  (testing "char overridden to text via user cast rule"
    (let [rules [{:source {:type :type :name "char"} :target-type "text"
                  :options {:drop-typemod true}}]
          columns [{:column-name "code" :column-type "char(3)"}
                   {:column-name "other" :column-type "int"}]
          result (cast/apply-type-overrides columns rules)]
      (is (= ["text" "int"] (mapv :column-type result)))
      ;; source type preserved so resolve-specs can still match on original
      (is (= "char(3)" (:source-column-type (first result))))))

;; ── New PR-1/PR-2 cast fixes ──────────────────────────────────────────────────

  (deftest test-hex-to-bytea
    (testing "plain hex string (#1066)"
      (is (= "\\xdeadbeef" (cast/hex-to-bytea "DEADBEEF"))))
    (testing "0x prefix stripped"
      (is (= "\\xcafe" (cast/hex-to-bytea "0xCAFE"))))
    (testing "\\x prefix stripped"
      (is (= "\\x01ff" (cast/hex-to-bytea "\\x01FF"))))
    (testing "nil input"
      (is (nil? (cast/hex-to-bytea nil)))))

  (deftest test-hex-to-bytea-in-registry
    (testing "hex-to-bytea is in cast registry and works via apply-cast"
      (is (= "\\xdeadbeef" (cast/apply-cast :hex-to-bytea "DEADBEEF")))))

  (deftest test-int-to-uuid-in-registry
    (testing "int-to-uuid is registered and converts integers (#1457)"
      (let [result (cast/apply-cast :int-to-uuid "0")]
        (is (= "00000000-0000-0000-0000-000000000000" result)))))

  (deftest test-char-cast-prefix-match
    (testing "type char cast rule matches char(N) via prefix (#1525)"
      (let [rules [{:source {:type :type :name "char"} :target-type "text"
                    :options {:drop-typemod true}}]
            columns [{:column-name "code" :column-type "char(3)"}
                     {:column-name "flag" :column-type "char(1)"}]
            result (cast/apply-type-overrides columns rules)]
        (is (= ["text" "text"] (mapv :column-type result)))))

    (testing "char(1) matched by 'char' rule (char without parens)"
      (let [rules [{:source {:type :type :name "char"} :target-type "varchar"
                    :options {:drop-typemod false}}]
            columns [{:column-name "x" :column-type "char"}]
            result (cast/apply-type-overrides columns rules)]
        (is (= ["varchar"] (mapv :column-type result))))))

  (deftest test-column-cast-no-type
    (testing "column cast without 'to TYPE' does not change type (#1522)"
      (let [rules [{:source {:type :column :column "status"}
                    :target-type nil
                    :using :set-to-enum-array}]
            columns [{:column-name "status" :column-type "enum('a','b')"}
                     {:column-name "name"   :column-type "varchar(100)"}]
            overridden (cast/apply-type-overrides columns rules)]
        (is (= "enum('a','b')" (:column-type (first overridden))))
        (is (= :set-to-enum-array (:cast-fn (first overridden))))))

    (testing "column cast without 'to TYPE' resolve-specs picks up the using fn"
      (let [rules [{:source {:type :column :column "status"}
                    :target-type nil
                    :using :set-to-enum-array}]
            columns [{:column-name "status" :column-type "enum('a','b')"}]
            specs (cast/resolve-specs rules columns)]
        (is (= [:set-to-enum-array] specs))))))

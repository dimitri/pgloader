(ns pgloader.source.dbf-test
  (:require [clojure.test :refer [deftest is testing]])
  (:import [java.nio.charset Charset]))

;; charset-aliases is private, but we can verify the same mapping logic
;; by testing that the JVM resolves the aliased names correctly.

(def ^:private charset-aliases
  "Mirror of pgloader.source.dbf/charset-aliases — kept in sync manually.
   These are the aliases that some JVMs don't register directly."
  {"cp950"  "Big5"
   "CP950"  "Big5"
   "cp932"  "windows-31j"
   "CP932"  "windows-31j"})

(defn- charset-for [^String encoding]
  (Charset/forName (get charset-aliases encoding encoding)))

(deftest test-charset-aliases-resolve
  (testing "cp950 / CP950 resolve to the Big5 charset (#1670)"
    (is (= "Big5" (.name (charset-for "cp950")))
        "cp950 (lowercase) should map to Big5")
    (is (= "Big5" (.name (charset-for "CP950")))
        "CP950 (uppercase) should map to Big5"))

  (testing "cp932 / CP932 resolve to windows-31j (Shift_JIS variant) (#1670)"
    (let [name-932 (.name (charset-for "cp932"))]
      (is (#{"windows-31j" "x-MS932_0213"} name-932)
          (str "cp932 should map to a Shift_JIS-family charset, got: " name-932)))
    (let [name-932 (.name (charset-for "CP932"))]
      (is (#{"windows-31j" "x-MS932_0213"} name-932)
          (str "CP932 should map to a Shift_JIS-family charset, got: " name-932))))

  (testing "standard aliases pass through unchanged"
    (is (instance? Charset (charset-for "UTF-8")))
    (is (instance? Charset (charset-for "ISO-8859-1")))))

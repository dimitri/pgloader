(ns pgloader.pg-service-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.java.io :as io]
            [pgloader.pg-service :as svc]))

;; ── helpers ───────────────────────────────────────────────────────────────────

(defn- with-pgpass-file [contents f]
  (let [tmp (java.io.File/createTempFile "pgpass" nil)]
    (try
      (spit tmp contents)
      (System/setProperty "PGPASSFILE_TEST" (.getAbsolutePath tmp))
      (with-redefs [pgloader.pg-service/pgpass-file-path (constantly (.getAbsolutePath tmp))]
        (f))
      (finally
        (.delete tmp)))))

;; ── pgpass-lookup ─────────────────────────────────────────────────────────────

(deftest test-pgpass-lookup-exact-match
  (with-pgpass-file "localhost:5432:mydb:myuser:s3cr3t\n"
    (fn []
      (is (= "s3cr3t" (svc/pgpass-lookup "localhost" 5432 "mydb" "myuser"))))))

(deftest test-pgpass-lookup-wildcard-host
  (with-pgpass-file "*:5432:mydb:myuser:pass1\n"
    (fn []
      (is (= "pass1" (svc/pgpass-lookup "anyhost" 5432 "mydb" "myuser"))))))

(deftest test-pgpass-lookup-wildcard-all
  (with-pgpass-file "*:*:*:*:defaultpass\n"
    (fn []
      (is (= "defaultpass" (svc/pgpass-lookup "h" 5432 "d" "u"))))))

(deftest test-pgpass-lookup-no-match
  (with-pgpass-file "otherhost:5432:mydb:myuser:pass\n"
    (fn []
      (is (nil? (svc/pgpass-lookup "localhost" 5432 "mydb" "myuser"))))))

(deftest test-pgpass-lookup-first-match-wins
  (with-pgpass-file "localhost:5432:mydb:myuser:first\nlocalhost:5432:mydb:myuser:second\n"
    (fn []
      (is (= "first" (svc/pgpass-lookup "localhost" 5432 "mydb" "myuser"))))))

(deftest test-pgpass-lookup-skips-comments
  (with-pgpass-file "# comment line\nlocalhost:5432:mydb:myuser:thepass\n"
    (fn []
      (is (= "thepass" (svc/pgpass-lookup "localhost" 5432 "mydb" "myuser"))))))

(deftest test-pgpass-lookup-missing-file
  (with-redefs [pgloader.pg-service/pgpass-file-path (constantly "/nonexistent/.pgpass")]
    (is (nil? (svc/pgpass-lookup "localhost" 5432 "mydb" "myuser")))))

;; ── apply-pgpass ──────────────────────────────────────────────────────────────

(deftest test-apply-pgpass-fills-missing-password
  (with-pgpass-file "localhost:5432:testdb:testuser:secret\n"
    (fn []
      (let [uri {:type :pgsql :host "localhost" :port 5432 :db "testdb" :user "testuser"}
            result (svc/apply-pgpass uri)]
        (is (= "secret" (:password result)))))))

(deftest test-apply-pgpass-preserves-existing-password
  (with-pgpass-file "*:*:*:*:shouldnotbeused\n"
    (fn []
      (let [uri {:type :pgsql :host "localhost" :port 5432 :db "db" :user "u" :password "existing"}
            result (svc/apply-pgpass uri)]
        (is (= "existing" (:password result)))))))

(deftest test-apply-pgpass-no-match-returns-uri-unchanged
  (with-pgpass-file "otherhost:5432:db:user:pass\n"
    (fn []
      (let [uri {:type :pgsql :host "localhost" :port 5432 :db "db" :user "user"}
            result (svc/apply-pgpass uri)]
        (is (nil? (:password result)))
        (is (= uri (dissoc result :password)))))))

(ns pgloader.cli-test
  (:require [clojure.test :refer [deftest is testing]]
            [pgloader.cli :as cli]))

(deftest test-parse-args-basic
  (testing "positional .load file"
    (let [opts (cli/parse-args ["myfile.load"])]
      (is (= ["myfile.load"] (:load-files opts)))))
  (testing "source and target URIs"
    (let [opts (cli/parse-args ["mysql://h/db" "pgsql://h/t"])]
      (is (= "mysql://h/db" (:source-uri opts)))
      (is (= "pgsql://h/t" (:target-uri opts)))))
  (testing "verbose flag"
    (let [opts (cli/parse-args ["--verbose" "f.load"])]
      (is (true? (:verbose opts)))))
  (testing "quiet flag"
    (let [opts (cli/parse-args ["--quiet" "f.load"])]
      (is (true? (:quiet opts)))))
  (testing "root-dir"
    (let [opts (cli/parse-args ["--root-dir" "/tmp" "f.load"])]
      (is (= "/tmp" (:root-dir opts)))))
  (testing "summary"
    (let [opts (cli/parse-args ["--summary" "out.csv" "f.load"])]
      (is (= "out.csv" (:summary opts))))))

(deftest test-parse-args-type
  (testing "--type sets source-type"
    (let [opts (cli/parse-args ["--type" "csv" "source.csv" "pgsql://h/t"])]
      (is (= "csv" (:source-type opts))))))

(deftest test-parse-args-encoding
  (testing "--encoding sets encoding"
    (let [opts (cli/parse-args ["--encoding" "latin-1" "mysql://h/db" "pgsql://h/t"])]
      (is (= "latin-1" (:encoding opts))))))

(deftest test-parse-args-with
  (testing "--with accumulates options"
    (let [opts (cli/parse-args ["--with" "create tables" "--with" "reset sequences"
                                "mysql://h/db" "pgsql://h/t"])]
      (is (= ["create tables" "reset sequences"] (:with-opts opts))))))

(deftest test-parse-args-cast
  (testing "--cast accumulates cast rules"
    (let [opts (cli/parse-args ["--cast" "column foo to integer"
                                "mysql://h/db" "pgsql://h/t"])]
      (is (= ["column foo to integer"] (:cast-rules opts))))))

(deftest test-parse-args-before-after
  (testing "--before sets before-file"
    (let [opts (cli/parse-args ["--before" "setup.sql" "mysql://h/db" "pgsql://h/t"])]
      (is (= "setup.sql" (:before-file opts)))))
  (testing "--after sets after-file"
    (let [opts (cli/parse-args ["--after" "cleanup.sql" "mysql://h/db" "pgsql://h/t"])]
      (is (= "cleanup.sql" (:after-file opts))))))

(deftest test-parse-args-set
  (testing "--set var to val collects set-params"
    (let [opts (cli/parse-args ["--set" "search_path" "to" "myschema"
                                "mysql://h/db" "pgsql://h/t"])]
      (is (= [["search_path" "myschema"]] (:set-params opts))))))

(deftest test-parse-args-multiple-flags
  (testing "multiple CLI overrides together"
    (let [opts (cli/parse-args ["--type" "mysql"
                                "--with" "create tables"
                                "--with" "reset sequences"
                                "--encoding" "utf-8"
                                "mysql://h/db" "pgsql://h/t"])]
      (is (= "mysql" (:source-type opts)))
      (is (= ["create tables" "reset sequences"] (:with-opts opts)))
      (is (= "utf-8" (:encoding opts))))))

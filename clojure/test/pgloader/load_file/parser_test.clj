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

(deftest test-parse-cast-when-default-and-not-null
  (testing "CAST ... when default \"...\" and not null parses (#1676)"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM mysql://user@localhost/mydb
                    INTO postgresql:///target
                    CAST type datetime when default \"0000-00-00 00:00:00\" and not null
                           to timestamp drop not null drop default using zero-dates-to-null;")]
      (is (:ok result))
      (let [rule (first (:cast-rules (:ok result)))]
        (is (= "0000-00-00 00:00:00" (:when-default rule)))
        (is (true? (:when-not-null rule)))))))

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

(deftest test-parse-csv-with-default-date-format
  (testing "WITH date format applies to typed target date columns and explicit field formats override it"
    (let [result (parser/parse-string
                  "LOAD CSV FROM '/data/dates.csv'
                     (id, created_at, closed_at [date format 'DD/MM/YYYY'])
                   INTO postgresql:///target
                   TARGET TABLE public.events
                     (id integer, created_at timestamptz, closed_at date)
                   WITH date format 'YYYY-MM-DD HH24-MI-SS.US';")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [cmd (:ok result)
            formats (get-in cmd [:source :column-formats])]
        (is (= "YYYY-MM-DD HH24-MI-SS.US"
               (some #(when (= "created_at" (:name %)) (:date-format %))
                     formats)))
        (is (= "DD/MM/YYYY"
               (some #(when (= "closed_at" (:name %)) (:date-format %))
                     formats)))
        (is (nil? (some #(when (= "id" (:name %)) (:date-format %))
                        formats)))))))

(deftest test-parse-csv-default-date-format-matches-target-names
  (testing "WITH date format does not fall back to target projection positions"
    (let [result (parser/parse-string
                  "LOAD CSV FROM '/data/dates.csv'
                     (id, name, created_at)
                   INTO postgresql:///target
                   TARGET TABLE public.events
                     (id integer, created_at timestamptz, name text)
                   WITH date format 'YYYY-MM-DD';")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [formats (get-in result [:ok :source :column-formats])]
        (is (= "YYYY-MM-DD"
               (some #(when (= "created_at" (:name %)) (:date-format %))
                     formats)))
        (is (nil? (some #(when (= "name" (:name %)) (:date-format %))
                        formats)))))))

(deftest test-parse-fixed-with-date-format
  (testing "LOAD FIXED accepts WITH date format"
    (let [result (parser/parse-string
                  "LOAD FIXED FROM fixed:///data/events.dat
                     (id from 0 for 2, created_at from 2 for 10)
                   INTO postgresql:///target
                   WITH date format 'YYYY-MM-DD';")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [cmd (:ok result)]
        (is (= :fixed (:load-type cmd)))
        (is (= "YYYY-MM-DD"
               (get-in cmd [:with-options :date-format])))))))

(deftest test-parse-fixed-with-date-format-applies-to-columns
  (testing "LOAD FIXED WITH date format populates column-formats for typed target date fields"
    (let [result (parser/parse-string
                  "LOAD FIXED FROM fixed:///data/events.dat
                     (id from 0 for 2, created_at from 2 for 10)
                   INTO postgresql:///target
                   TARGET TABLE public.events
                     (id integer, created_at timestamptz)
                   WITH date format 'YYYY-MM-DD';")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [cmd (:ok result)
            formats (get-in cmd [:source :column-formats])]
        (is (= "YYYY-MM-DD"
               (some #(when (= "created_at" (:name %)) (:date-format %))
                     formats)))
        (is (nil? (some #(when (= "id" (:name %)) (:date-format %))
                        formats)))))))

;; ── CSV null-if tests (issues #1135, #1221) ─────────────────────────────────

(deftest test-parse-csv-null-if-blanks-per-column
  (testing "per-column [null if blanks] is parsed (issue #1135)"
    (let [result (parser/parse-string
                  "LOAD CSV FROM '/data/test.csv'
                     (id, name [null if blanks], score)
                   INTO postgresql:///target;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [source  (get-in result [:ok :source])
            col-nif (:column-nullifs source)]
        (is (seq col-nif))
        (let [name-col (first (filter #(= "name" (:name %)) col-nif))]
          (is name-col)
          (is (= [:blanks] (:nullifs name-col))))))))

(deftest test-parse-csv-multiple-null-if-per-column
  (testing "multiple [null if] specs per column parsed as a list (issue #1221)"
    (let [result (parser/parse-string
                  "LOAD CSV FROM '/data/test.csv'
                     (id, score [null if blanks, null if '0'], name)
                   INTO postgresql:///target;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [source  (get-in result [:ok :source])
            col-nif (:column-nullifs source)]
        (is (seq col-nif))
        (let [score-col (first (filter #(= "score" (:name %)) col-nif))]
          (is score-col)
          (is (= [:blanks "0"] (:nullifs score-col))))))))

(deftest test-parse-csv-multiple-null-if-separate-brackets
  (testing "multiple separate [null if] bracket groups per column also work"
    (let [result (parser/parse-string
                  "LOAD CSV FROM '/data/test.csv'
                     (id, code [null if ''] [null if 'N/A'])
                   INTO postgresql:///target;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [source  (get-in result [:ok :source])
            col-nif (:column-nullifs source)]
        (is (seq col-nif))
        (let [code-col (first (filter #(= "code" (:name %)) col-nif))]
          (is code-col)
          (is (= ["" "N/A"] (:nullifs code-col))))))))

(deftest test-parse-csv-http-source
  (testing "CSV FROM unquoted http URL is parsed as :csv type with :url (issue #1606)"
    (let [result (parser/parse-string
                  "LOAD CSV FROM https://example.com/data.csv?token=abc
                   INTO postgresql:///target;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [source (get-in result [:ok :source])]
        (is (= :csv (:type source)))
        (is (= "https://example.com/data.csv?token=abc" (:url source)))))))

(deftest test-parse-archive-csv-filename-matching
  (testing "FILENAME MATCHING inside LOAD ARCHIVE CSV sub-command (issue #1335)"
    (let [result (parser/parse-string
                  "LOAD ARCHIVE FROM 'data.zip'
                   INTO postgresql:///target
                   LOAD CSV FROM FILENAME MATCHING ~/zone\\.csv/
                     (zone_id, country_code, zone_name)
                   INTO postgresql:///target;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (let [cmd (get-in result [:ok :commands 0])
            source (:source cmd)]
        (is (= :csv (:type source)))
        (is (= "zone\\.csv" (:filename-pattern source)))))))

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

(deftest test-expand-env
  (testing "{{VAR}} is replaced with the OS environment variable value"
    (let [home (System/getenv "HOME")]
      (is (= (str "path=" home)
             (#'parser/expand-env "path={{HOME}}")))))
  (testing "multiple placeholders expanded in one pass"
    (let [home (System/getenv "HOME")
          user (System/getenv "USER")]
      (is (= (str home ":" user)
             (#'parser/expand-env "{{HOME}}:{{USER}}")))))
  (testing "undefined {{VAR}} throws with informative message"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"Undefined template variable: PGLOADER_NO_SUCH_VAR_XYZ"
                          (#'parser/expand-env "{{PGLOADER_NO_SUCH_VAR_XYZ}}"))))
  (testing "no placeholders passes through unchanged"
    (is (= "plain string" (#'parser/expand-env "plain string")))))

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

;; ── EXCLUDING / INCLUDING table filters (#1328) ───────────────────────────────

(deftest test-excluding-names-like
  (testing "EXCLUDING TABLE NAMES LIKE 'pat' parses (alias for MATCHING)"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM mssql://h/db INTO pgsql://h/t
                    EXCLUDING TABLE NAMES LIKE 'tmp_%';")]
      (is (:ok result) (str "Parse error: " (:error result)))
      (is (= ["tmp..*"] (get-in result [:ok :filters :excluding])))))

  (testing "EXCLUDING TABLE NAMES MATCHING multiple patterns (#1328)"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM mssql://h/db INTO pgsql://h/t
                    EXCLUDING TABLE NAMES MATCHING 'tmp_%', 'bak_%';")]
      (is (:ok result) (str "Parse error: " (:error result)))
      (is (= ["tmp..*" "bak..*"]
             (get-in result [:ok :filters :excluding])))))

  (testing "Multiple EXCLUDING clauses are all collected"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM mssql://h/db INTO pgsql://h/t
                    EXCLUDING TABLE NAMES LIKE 'tmp_%'
                    EXCLUDING TABLE NAMES LIKE 'bak_%';")]
      (is (:ok result) (str "Parse error: " (:error result)))
      (is (= ["tmp..*" "bak..*"]
             (get-in result [:ok :filters :excluding])))))

  (testing "INCLUDING TABLE NAMES LIKE parses single pattern"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM mssql://h/db INTO pgsql://h/t
                    INCLUDING ONLY TABLE NAMES LIKE 'orders';")]
      (is (:ok result) (str "Parse error: " (:error result)))
      (is (= ["orders"] (get-in result [:ok :filters :including]))))))

;; ── Cast grammar fixes (#1522, #1273, #1470) ─────────────────────────────────

(deftest test-cast-column-no-type
  (testing "CAST column col using fn (no 'to TYPE') parses successfully (#1522)"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    CAST column tbl.status using set-to-enum-array;")]
      (is (:ok result) (str "Parse error: " (:error result)))
      (let [rule (first (get-in result [:ok :cast-rules]))]
        (is (nil? (:target-type rule)))
        (is (= :set-to-enum-array (:using rule))))))

  (testing "CAST column col (no 'to TYPE', no using) parses successfully"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    CAST column tbl.status drop not null;")]
      (is (:ok result) (str "Parse error: " (:error result)))
      (let [rule (first (get-in result [:ok :cast-rules]))]
        (is (nil? (:target-type rule)))))))

(deftest test-cast-target-type-multi-word
  (testing "multi-word target type like 'timestamp without time zone' parses (#1273)"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    CAST type timestamp to \"timestamp without time zone\";")]
      (is (:ok result) (str "Parse error: " (:error result)))
      (let [rule (first (get-in result [:ok :cast-rules]))]
        (is (= "timestamp without time zone" (:target-type rule))))))

  (testing "unquoted multi-word type like 'timestamp without time zone' parses (#1273)"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    CAST type timestamp to timestamp without time zone;")]
      (is (:ok result) (str "Parse error: " (:error result)))
      (let [rule (first (get-in result [:ok :cast-rules]))]
        (is (= "timestamp without time zone" (:target-type rule))))))

  (testing "timestamp to plain timestamp cast overrides default timestamptz (#1470)"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM mysql://h/db INTO pgsql://h/t
                    CAST type timestamp to timestamp;")]
      (is (:ok result) (str "Parse error: " (:error result)))
      (let [rule (first (get-in result [:ok :cast-rules]))]
        (is (= "timestamp" (:target-type rule)))))))

;; ---------------------------------------------------------------------------
;; #1365 — {{VAR}} with whitespace in the value (paths with spaces)
;; ---------------------------------------------------------------------------

(deftest test-single-quoted-source-uri-grammar
  (testing "grammar accepts single-quoted source-uri — needed for paths with spaces (#1365)"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM 'sqlite:///path/with spaces/my db.sqlite'
                   INTO postgresql://localhost/target;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (is (= :sqlite (get-in result [:ok :source :type])))
      (is (= "/path/with spaces/my db.sqlite" (get-in result [:ok :source :path]))))))

(deftest test-single-quoted-pg-uri-grammar
  (testing "grammar accepts single-quoted pg-uri (#1365)"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM sqlite:///tmp/test.db
                   INTO 'postgresql://user@localhost/my database';")]
      (is (:ok result) (str "Parse failed: " (:error result))))))

(deftest test-var-expansion-with-space-using-quoted-uri
  (testing "{{VAR}} expanding to a path with a space works when URI is quoted (#1365)
            Pattern: FROM 'sqlite:///{{DB_PATH}}' where DB_PATH has a space.
            After expansion: FROM 'sqlite:///my data/db.sqlite'."
    (let [;; Simulate the result of expand-env on 'sqlite:///{{DB_PATH}}'
          ;; where DB_PATH = 'my data/db.sqlite'
          expanded "LOAD DATABASE FROM 'sqlite:///my data/db.sqlite'
                    INTO postgresql://localhost/tgt;"
          result   (parser/parse-string expanded)]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (is (= "/my data/db.sqlite" (get-in result [:ok :source :path]))))))

(deftest test-unquoted-plain-path-still-works
  (testing "unquoted path without spaces continues to work (#1365 non-regression)"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM sqlite:///tmp/plain.db
                   INTO postgresql://localhost/tgt;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (is (= "/tmp/plain.db" (get-in result [:ok :source :path]))))))

;; ── SQLite CLI --with parity: grammar support for identifier-case (#1187) ───

(deftest test-sqlite-with-quote-identifiers-grammar
  (testing "LOAD DATABASE FROM sqlite://... WITH quote identifiers parses (#1187)"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM sqlite:///tmp/foo.db
                   INTO postgresql:///target
                   WITH quote identifiers;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (is (true? (get-in result [:ok :with-options :quote-ids])))))

  (testing "LOAD DATABASE FROM sqlite://... WITH snake_case identifiers parses"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM sqlite:///tmp/foo.db
                   INTO postgresql:///target
                   WITH snake_case identifiers;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (is (true? (get-in result [:ok :with-options :snake-case-ids])))))

  (testing "LOAD DATABASE FROM sqlite://... WITH downcase identifiers parses"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM sqlite:///tmp/foo.db
                   INTO postgresql:///target
                   WITH downcase identifiers;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (is (true? (get-in result [:ok :with-options :downcase-ids]))))))

(deftest test-with-on-error-stop
  (testing "WITH on error stop sets :on-error-stop true in with-options"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM sqlite:///tmp/foo.db
                   INTO postgresql:///target
                   WITH on error stop;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (is (true? (get-in result [:ok :with-options :on-error-stop])))))

  (testing "WITH on error resume next does not set :on-error-stop"
    (let [result (parser/parse-string
                  "LOAD DATABASE FROM sqlite:///tmp/foo.db
                   INTO postgresql:///target
                   WITH on error resume next;")]
      (is (:ok result) (str "Parse failed: " (:error result)))
      (is (nil? (get-in result [:ok :with-options :on-error-stop]))))))

# pgloader v4 — Clojure Rewrite Specification

**Version:** 2.0  
**Status:** Implementation-ready specification  
**Author:** Dimitri Fontaine  
**Intended reader:** Claude Code (autonomous implementation agent)

---

## 0. How to Use This Document

Read this document completely before writing any code. It is structured as:

1. **Guiding principles** — read once, apply everywhere
2. **Repository setup** — do first, before any source
3. **Build environment** — verify before writing code
4. **Dependency map** — reference when writing `deps.edn`
5. **Source protocol** — the abstraction all sources implement
6. **SQL files and HugSQL** — how introspection queries are stored and loaded
7. **v4 Error Handling Architecture** — batch binary search, reject files
8. **v4 Encoding Strategy** — per-source encoding, UTF-8 internal
9. **v4 Prefetch Buffering** — producer-consumer batch pipeline
10. **Phase 1 specification** — implement this before anything else (CSV + MySQL → PostgreSQL prototype with load file parser)
11. **Full phase plan** — Phases 2–6, implement after Phase 1 passes all tests
12. **File format library reference** — use when implementing file-based sources
13. **Oracle in Docker** — assessment and setup notes
14. **GraalVM build specification** — add last, after all tests pass

Do not start Phase 2 until Phase 1 integration tests pass. Do not attempt GraalVM build until all phases are complete.

---

## 1. Guiding Principles

### Language and Runtime
- **Clojure 1.12** on **JDK 21**. No other JVM language.
- Build: **`deps.edn`** with `tools.build` for the uberjar. No Leiningen.
- Target: **GraalVM native binary** (single executable, ~12ms startup, ~30MB). Phase 6.
- No `eval`, no `proxy`, no dynamic class loading. Resolvable at AOT compile time.
- Use `^Type` type hints on every Java interop call. Set `(set! *warn-on-reflection* true)` in every namespace with Java interop.

### Data Path
- Row data **never accumulates in memory** beyond the current batch and prefetch queue.
- The COPY protocol buffer (`CopyIn`) is the only place rows accumulate during write, and only for the COPY TEXT encoding step.
- `pgjdbc CopyManager` is the **only** mechanism for writing to PostgreSQL. No `INSERT`, no batch, no `next.jdbc/execute!` for data.

### Concurrency
- Use **JDK 21 virtual threads**: `(Thread/startVirtualThread ...)` or `Executors/newVirtualThreadPerTaskExecutor`.
- One reader virtual thread per table copy. One writer virtual thread per table copy.
- Partitioned reads (same table, row ranges) get paired reader+writer virtual threads.
- The main thread orchestrates and collects results via `Future/get`.
- Do **not** use `core.async`. Do **not** use `future`. Use virtual threads directly.

### Error Handling
- Every table copy runs in its own virtual thread with its own try/catch.
- **Per-batch transactions**: each batch of rows is sent as `BEGIN COPY ... COMMIT`. On failure: `ROLLBACK` + binary search retry.
- Errors per row are collected into reject files: `<table>.reject.dat` (raw row bytes) and `<table>.reject.log` (error text).
- Default: `on-error continue`. Configurable to `stop` (passes exceptions to the orchestrator, halting the table copy).
- Errors never cross table boundaries.

### Configuration and Load Files
- The `.load` file format is the existing pgloader DSL. Implement a parser using **instaparse**.
- The load file parser is included in Phase 1 — not deferred. Inline CLI args (`mysql://... pgsql://...`) are sugar that generates a synthetic load command internally.
- Parser tests are a Phase 1 acceptance criterion.

### Cast Functions
- All cast functions are pure `(fn [^String v] ^String)` returning a String or nil.
- Registered in a map `{:name fn}` at startup. No reflection, no dynamic dispatch.

### SQL Files
- All schema introspection SQL is stored in `.sql` files under `resources/sql/`. Never inline SQL strings in Clojure source.
- **HugSQL** loads `.sql` files and binds them as Clojure functions via `next.jdbc` adapter.

---

## 2. Repository Setup

### 2.1 Git Operations

The new code lives in the **same repository** as the existing pgloader, on a new branch.

```bash
cd pgloader
git checkout -b v4-clojure-rewrite
mkdir -p clojure/src clojure/test clojure/resources
```

**Keep from the existing repo:**
- `docs/` — all documentation
- `test/` — existing load files as reference fixtures
- `src/` — existing CL source as read-only reference
- `pgloader.asd`, `Makefile`, `Dockerfile` — unmodified

**Add on this branch:**
- `clojure/` — the entire new implementation

**Never modify** any file outside `clojure/`. Never modify existing CL source files.

### 2.2 Directory Structure

```
pgloader/
├── src/                  ← existing CL (read-only)
├── test/                 ← existing .load fixtures
├── docs/                 ← existing docs
└── clojure/
    ├── deps.edn
    ├── build.clj
    ├── docker-compose.yml
    ├── Makefile
    ├── README.md
    ├── resources/
    │   └── sql/
    │       ├── mysql/
    │       ├── mssql/
    │       ├── sqlite/
    │       └── postgresql/
    ├── src/pgloader/
    │   ├── core.clj
    │   ├── cli.clj
    │   ├── load_file/
    │   │   ├── parser.clj      ← instaparse grammar + parse fn
    │   │   ├── grammar.clj     ← grammar string as a def
    │   │   └── ast.clj         ← hiccup → load command record
    │   ├── cast.clj
    │   ├── copy.clj
    │   ├── batch.clj           ← Batch record, fill, send, retry
    │   ├── prefetch.clj        ← reader/writer pipeline
    │   ├── reject.clj          ← reject file writer
    │   ├── progress.clj
    │   ├── summary.clj
    │   ├── source/
    │   │   ├── protocol.clj
    │   │   ├── csv.clj
    │   │   ├── mysql.clj
    │   │   ├── mssql.clj
    │   │   ├── sqlite.clj
    │   │   ├── postgresql.clj
    │   │   └── dbf.clj
    │   └── ddl/
    │       ├── mysql.clj
    │       ├── mssql.clj
    │       ├── sqlite.clj
    │       └── common.clj
    └── test/pgloader/
        ├── load_file/
        │   ├── parser_test.clj
        │   └── fixtures/
        ├── batch_test.clj
        ├── cast_test.clj
        ├── copy_test.clj
        └── integration/
```

### 2.3 deps.edn

```clojure
{:paths ["src" "resources"]

 :deps
 {org.clojure/clojure       {:mvn/version "1.12.0"}
  org.clojure/tools.cli     {:mvn/version "0.4.2"}
  org.clojure/tools.logging {:mvn/version "1.3.0"}
  instaparse/instaparse     {:mvn/version "1.4.12"}
  com.layerware/hugsql-core                 {:mvn/version "0.5.3"}
  com.layerware/hugsql-adapter-next-jdbc    {:mvn/version "0.5.3"}
  com.github.seancorfield/next.jdbc {:mvn/version "1.3.939"}
  com.github.seancorfield/honeysql  {:mvn/version "2.6.1147"}
  org.postgresql/postgresql {:mvn/version "42.7.3"}
  com.mysql/mysql-connector-j {:mvn/version "9.0.0"}
  com.microsoft.sqlserver/mssql-jdbc {:mvn/version "12.6.1.jre11"}
  org.xerial/sqlite-jdbc    {:mvn/version "3.46.0.0"}
  com.opencsv/opencsv       {:mvn/version "5.9"}
  com.github.albfernandez/javadbf {:mvn/version "1.13.1"}
  ch.qos.logback/logback-classic {:mvn/version "1.5.6"}
  io.github.clj-easy/graal-build-time {:mvn/version "1.0.5"}}

 :aliases
 {:test
  {:extra-paths ["test"]
   :extra-deps  {io.github.cognitect-labs/test-runner
                 {:git/url "https://github.com/cognitect-labs/test-runner"
                  :sha     "..."}}}
  :build
  {:deps {io.github.clojure/tools.build {:mvn/version "0.10.3"}}
   :ns-default build}
  :native
  {:jvm-opts ["-agentlib:native-image-agent=config-output-dir=graalvm-config"]}}}
```

---

## 3. Build Environment

Same as v1.1 spec: JDK 21, Clojure CLI 1.11+, Docker 24+, Docker Compose v2. See `Makefile` `check-env` target. No change needed from the previous spec.

---

## 4. Source Protocol

```clojure
(defprotocol Source
  (source-name [this]
    "Human-readable name for progress reporting.")

  (catalog [this]
    "Return a sequence of TableSpec describing what to copy.
     Each map: {:source-table string
                :target-schema string
                :target-table  string
                :columns       [{:name :source-type :target-type
                                 :cast :nullable :default :extra}]
                :options       map}")

  (read-rows [this table-spec]
    "Return a lazy sequence of rows (vectors of String-or-nil).
     Must be safe to call concurrently for different table-specs.")

  (close! [this]
    "Release all resources held by this source."))
```

Every source implements this protocol. New sources add one namespace.

---

## 5. SQL Files and HugSQL

Same as v1.1 spec. SQL files in `resources/sql/<source-type>/<query>.sql`, loaded via HugSQL's `def-db-fns`. Extract SQL from existing `src/sources/*/db-objects.lisp`. Add `-- :name` and `-- :doc` annotations. Replace positional `?` with HugSQL `:param` syntax. See v1.1 spec Section 6 for conventions and file list.

---

## 6. Load File Parser

### 6.1 DSL Architecture

The pgloader DSL has grown over 15 years. It is a **flat command structure** with three load types and a shared pool of clause types:

```
COMMAND := LOAD <load-type> FROM <source> INTO <target>
           [WITH <option-list>]
           [SET <parameter-list>]
           [CAST <cast-rules>]
           [INCLUDING ONLY TABLE NAMES <filter>]
           [EXCLUDING TABLE NAMES <filter>]
           [ALTER SCHEMA <schema> RENAME TO <schema>]
           [ALTER TABLE <table> RENAME COLUMN <col> TO <col>]
           [MATERIALIZE VIEWS <views>]
           [BEFORE LOAD EXECUTE <sql>]
           [AFTER LOAD EXECUTE <sql>]
           ;

<load-type> := CSV | FIXED | DBF | IXF | COPY | ARCHIVE
             | DATABASE FROM mysql://...
             | DATABASE FROM mssql://...
             | DATABASE FROM sqlite://...
             | DATABASE FROM pgsql://...
```

**Key insight from the Esrap grammar:** the v3 parser uses a **command-specific optional clauses rule** per source type (`load-mysql-optional-clauses`, `load-mssql-optional-clauses`, etc.), each specifying which clauses are valid for that source. The alternatives are combined with `* (or ...)` making the order free. The same approach works in instaparse: each load-type rule defines its own clause set.

The Esrap structure decomposes cleanly into instaparse:
- **Keywords**: individual PEG rules matching case-insensitively (`~"load"` in Esrap, instaparse handles `:string-ci true`)
- **Clause rules**: one rule per clause type (`cast-clause`, `set-clause`, `including-clause`, etc.)
- **Type dispatch**: the top-level `command` rule tries each load-type in order
- **URI parsing**: delegated to Java's `java.net.URI` after the parser extracts the string — the grammar captures the full URI as a raw string

### 6.2 ESRA Parser Structure (for reference)

The v3 parser (Esrap) has this structure:

```
command.lisp (top level)
  ├── command-keywords.lisp  — keyword rules via def-keyword-rule macro
  ├── command-source.lisp    — file/URI source parsing
  ├── command-csv.lisp       — LOAD CSV grammar
  ├── command-fixed.lisp     — LOAD FIXED grammar
  ├── command-dbf.lisp       — LOAD DBF grammar
  ├── command-ixf.lisp       — LOAD IXF grammar
  ├── command-copy.lisp      — LOAD COPY grammar
  ├── command-archive.lisp   — LOAD ARCHIVE grammar
  ├── command-mysql.lisp     — LOAD DATABASE FROM mysql://
  ├── command-mssql.lisp     — LOAD DATABASE FROM mssql://
  ├── command-sqlite.lisp    — LOAD DATABASE FROM sqlite://
  ├── command-pgsql.lisp     — LOAD DATABASE FROM pgsql://
  ├── command-cast-rules.lisp — CAST rule grammar
  ├── command-options.lisp   — WITH option keywords
  ├── command-including.lisp — INCLUDING/EXCLUDING filters
  ├── command-alter-table.lisp — ALTER SCHEMA/TABLE
  ├── command-sql-block.lisp — BEFORE/AFTER LOAD EXECUTE
  ├── command-materialize-views.lisp
  └── command-utils.lisp     — shared parser utilities
```

For v4, the structure is flatter (instaparse grammar as a single string) but the **modular decomposition is the same**: each clause type gets its own grammar rule, and command-specific variants list their allowed clauses.

### 6.3 Phase 1 Grammar

See v1.1 spec Section 7.2 for the Phase 1 grammar (CSV + MySQL subset). Key differences from v1.1:

- **URI strings** are captured as raw text: `#'[a-zA-Z][a-zA-Z0-9+.-]*://\S+'` and parsed with `java.net.URI` in the AST step, not in the grammar. This avoids grammar bloat for DSN parsing and correctly handles edge cases (query parameters, IPv6 addresses, percent-encoded passwords).
- **Case insensitivity** is handled by instaparse's `:string-ci true` option — every keyword match is case-insensitive.
- **Comments**: `--` line comments are stripped in a pre-processing step before parsing, not handled in the grammar.

### 6.4 Parser Namespace

```clojure
(ns pgloader.load-file.parser
  (:require [instaparse.core :as insta]
            [pgloader.load-file.grammar :refer [grammar]]))

;; The grammar string is defined in pgloader.load-file.grammar
(def parse
  (insta/parser grammar
    :string-ci true
    :auto-whitespace :standard))

(defn parse-file [path]
  (let [content (-> path slurp strip-comments)
        result  (parse content)]
    (if (insta/failure? result)
      {:error (insta/get-failure result)}
      {:ok (ast/transform result)})))

(defn parse-string [s]
  (let [result (parse (strip-comments s))]
    (if (insta/failure? result)
      {:error (insta/get-failure result)}
      {:ok (ast/transform result)})))

(defn- strip-comments [s]
  ;; Remove -- line comments before parsing
  (clojure.string/replace s #"(?m)^\s*--[^\n]*\n?" ""))
```

### 6.5 AST Transformation

The hiccup tree from instaparse is transformed into a `LoadCommand` record:

```clojure
(defrecord LoadCommand
  [load-type       ; :csv | :database
   source          ; {:type :mysql :uri (java.net.URI.)}
   target          ; {:uri (java.net.URI.)}
   with-options    ; {:include-drop true :workers 4 ...}
   set-parameters  ; [{:var "maintenance_work_mem" :value "128MB"}]
   cast-rules      ; [{:source {:type :type :name "datetime"}
                      ; :target {:type "timestamptz"}
                      ; :options {:drop-not-null true :drop-default true}
                      ; :using :zero-dates-to-null}]
   filters         ; {:including [...] :excluding [...]}
   alter-schema    ; [{:from "dbo" :to "public"}]
   alter-table     ; [{:table "foo" :column "bar" :rename-to "baz"}]
   before-load     ; ["SQL string"]
   after-load      ; ["SQL string"]
   materialize-views [...]])
```

### 6.6 URI Parsing

URI strings from the grammar are parsed with `java.net.URI`:

```clojure
(defn parse-uri [^String uri-str]
  (let [uri (java.net.URI. uri-str)]
    (case (.getScheme uri)
      "mysql" {:type :mysql
               :host (or (.getHost uri) "localhost")
               :port (or (.getPort uri) 3306)
               :db (-> (.getPath uri) (subs 1))  ; strip leading /
               :user (when (.getUserInfo uri) (first (clojure.string/split (.getUserInfo uri) #":")))
               :password (when (.getUserInfo uri) (second (clojure.string/split (.getUserInfo uri) #":")))}
      "postgresql"
      "pgsql"
      (merge {:type :pgsql}
             (parse-jdbc-uri uri))
      "sqlite" {:type :sqlite
                :path (-> uri .getSchemeSpecificPart)}
      "csv" {:type :csv
             :path (-> uri .getSchemeSpecificPart)}
      ...)))
```

### 6.7 Parser Tests

Parser tests are a Phase 1 requirement. See v1.1 spec Section 7.3 for the full test file. All tests there apply unchanged. Add:

```
(deftest test-uri-with-password-special-chars
  ;; @ in password → URI parsing must handle percent-encoding
  (let [r (parser/parse-string
            "LOAD DATABASE FROM mysql://user:pass%40word@host/db
                    INTO postgresql://user@localhost/tgt ;")]
    (is (nil? (:error r)))))

(deftest test-uri-with-query-params
  ;; JDBC-style query params in URI
  (let [r (parser/parse-string
            "LOAD DATABASE FROM mysql://user@host/db?useSSL=true&requireSSL=true
                    INTO postgresql://user@localhost/tgt ;")]
    (is (nil? (:error r)))))
```

---

## 7. v4 Error Handling Architecture

### 7.1 Overview

This is the most critical subsystem. pgloader's distinguishing feature over `psql \copy` is that **a few bad rows never abort a table copy**. The mechanism: batch each COPY into isolated transactions and binary-search within failed batches to isolate bad rows.

```
┌─────────────────────────────────────────────────────┐
│                  Batch Lifecycle                     │
│                                                     │
│  [Reader] format-row → add to batch                 │
│       ↓ batch full or end-of-data                   │
│  [Queue] batch.put(queue)                           │
│       ↓                                             │
│  [Writer] batch ← queue.take()                      │
│       ↓                                             │
│  [Writer] BEGIN → CopyIn → endCopy → COMMIT         │
│       ↓ (on PSQLException)                          │
│  [Writer] ROLLBACK → retry-batch(batch)             │
│       ↓ (bad rows isolated, rest succeed)           │
│  [Writer] CONTINUE (next batch from queue)          │
└─────────────────────────────────────────────────────┘
```

### 7.2 Batch Record

```clojure
(defrecord Batch
  [^long start-time         ; System/nanoTime at creation
   ^long row-count          ; number of rows in this batch
   ^long byte-count         ; total bytes of all rows
   ^"[[B" rows              ; pre-formatted COPY TEXT line byte arrays
   ^long max-rows           ; capacity (randomized)
   ^long max-bytes]         ; byte limit
```

### 7.3 Batch Filling

```clojure
(defn make-batch
  ([] (make-batch *batch-rows* *batch-size*))
  ([max-rows max-bytes]
   (let [rand-rows (long (* max-rows (+ 0.7 (rand 0.6))))]  ; 0.7x–1.3x
     (Batch. (System/nanoTime) 0 0
             (make-array Byte/TYPE rand-rows)
             rand-rows max-bytes))))

(defn batch-full? [^Batch b]
  (or (>= (.row-count b) (.max-rows b))
      (>= (.byte-count b) (.max-bytes b))))

(defn batch-add-row! [^Batch b ^bytes row-bytes]
  (aset ^objects (.rows b) (.row-count b) row-bytes)
  (set! (.row-count b) (inc (.row-count b)))
  (set! (.byte-count b) (+ (.byte-count b) (alength row-bytes))))
```

### 7.4 Send Batch

```clojure
(defn send-batch! [^PGConnection pg-conn ^Batch batch table-spec]
  (let [sql (format "COPY %s.%s (%s) FROM STDIN WITH (FORMAT TEXT)"
                    (:target-schema table-spec)
                    (:target-table table-spec)
                    (str/join ", " (map :name (:columns table-spec))))
        cm   (.getCopyAPI pg-conn)
        ci   (.copyIn cm sql)]
    (try
      (dotimes [i (.row-count batch)]
        (let [^bytes row (aget ^objects (.rows batch) i)]
          (when row
            (.writeToCopy ci row 0 (alength row)))))
      (.endCopy ci)
      {:status :ok :rows (.row-count batch)}
      (catch PSQLException e
        (try (.endCopy ci) (catch Exception _))  ; discard failed COPY
        (throw e)))))
```

### 7.5 Binary Search Retry

```clojure
(defn retry-batch!
  "Binary-search retry within a failed batch.
   batch    — the Batch that had a row error
   table-sp — target table specification
   returns  — number of rows written to reject files"
  [^Batch batch table-sp ^PSQLException first-exception
   ^PGConnection pg-conn]
  (let [total-rows (.row-count batch)]
    (loop [pos 0
           next-error (parse-copy-line first-exception)
           errors 0]
      (if (>= pos total-rows)
        errors
        (let [err-line next-error]
          (cond
            ;; Single row identified as the error
            (= pos (dec err-line))
            (let [row (aget ^objects (.rows batch) pos)]
              (write-reject! row table-sp (current-exception))
              (recur (inc pos) (inc err-line) (inc errors)))

            ;; Try to send the good prefix [pos, err-line)
            (< pos (dec err-line))
            (try
              (send-partial-batch! pg-conn batch pos (dec err-line) table-sp)
              (recur err-line (inc err-line) errors)  ; skip error row
              (catch PSQLException e
                (recur pos (+ pos (parse-relative-line e)) errors)))

            ;; No error line known or pos > err-line: try remaining rows
            :else
            (try
              (send-partial-batch! pg-conn batch pos total-rows table-sp)
              (recur total-rows nil errors)
              (catch PSQLException e
                (let [new-err (+ pos (parse-relative-line e))]
                  (recur pos new-err errors))))))))))

(defn- send-partial-batch!
  [^PGConnection pg-conn ^Batch batch start end table-sp]
  ;; Opens a fresh COPY, sends rows [start, end), commits
  (with-open [ci (.copyIn (.getCopyAPI pg-conn) (copy-sql table-sp))]
    (loop [i start]
      (when (< i end)
        (let [^bytes row (aget ^objects (.rows batch) i)]
          (when row (.writeToCopy ci row 0 (alength row))))
        (recur (inc i))))
    (.endCopy ci)))

(defn- parse-copy-line [^PSQLException e]
  "Extract the 0-indexed failing line number from PostgreSQL error CONTEXT.
   CONTEXT: COPY errors, line 42, column x: ...
   Returns int."
  (let [msg (.getServerErrorMessage e)]
    (when msg
      (when-let [m (re-find #"COPY [^,]+, [^ ]+ (\d+)"
                            (.getMessage msg))]
        (- (Integer/parseInt (last m)) 1)))))  ; 1-indexed → 0-indexed
```

### 7.6 Reject Files

```clojure
(defn write-reject!
  [^bytes row-bytes table-spec ^PSQLException e]
  (let [dat-path (reject-dat-path (:target-schema table-spec)
                                  (:target-table table-spec))
        log-path (reject-log-path (:target-schema table-spec)
                                  (:target-table table-spec))]
    ;; Append row data (COPY TEXT format, raw bytes)
    (locking dat-path
      (with-open [w (java.io.FileOutputStream. dat-path true)]
        (.write w row-bytes)
        (.write w (int 10))))   ; \n
    ;; Append error message (UTF-8 text)
    (locking log-path
      (with-open [w (java.io.FileWriter. log-path true)]
        (.write w (str (.getMessage e) "\n"))))))
```

Reject files are per-table, appended to, never rotated. The v3 behavior of `root-dir` applies: reject files land under `~root-dir/<schema>/<table>.reject.dat` and `.reject.log`.

---

## 8. v4 Encoding Strategy

### 8.1 Principle

**All internal processing is UTF-8.** Encoding conversion happens at the boundary: when reading from a source and when writing to PostgreSQL COPY (which operates on UTF-8 bytes).

### 8.2 Per-Source Encoding

| Source | Strategy |
|---|---|
| **MySQL** | JDBC Connector/J handles wire encoding. Set `connectionEncoding=UTF-8` in JDBC URL. MySQL's `utf8mb3`/`utf8mb4` is transparent. |
| **MSSQL** | MSSQL JDBC sends Unicode natively (`sendStringParametersAsUnicode=true`, default). No conversion. |
| **SQLite** | SQLite stores TEXT as UTF-8 internally. JDBC returns UTF-8 strings directly. |
| **PostgreSQL→PostgreSQL** | `SET client_encoding TO 'UTF-8'` on target connection. Source encoding is whatever the source server reports — JDBC handles it. |
| **CSV / Fixed / DBF** | File encoding specified in the load file: `WITH ENCODING latin1`. Java `Charset.forName(name)` decodes the file. Default: UTF-8. |
| **VARBINARY / bytea** | No encoding conversion. Binary transforms (base64-decode, hex-decode) produce byte arrays. The `varbinary-to-string` transform uses ISO-8859-1 as the fallback encoding (safe for byte→string round-tripping). |

### 8.3 PostgreSQL Connection

Every target connection runs:
```sql
SET client_encoding TO 'UTF-8';
```

### 8.4 Row Formatting (COPY TEXT)

Rows are encoded to UTF-8 COPY TEXT byte arrays before queuing:

```clojure
(defn format-row [row cast-specs]
  (let [sb (StringBuilder.)]
    (doseq [i (range (count row))]
      (when (pos? i) (.append sb \tab))
      (if-let [v (nth row i)]
        (let [cast-fn (get cast-specs i)]
          (escape-copy-text sb (if cast-fn (cast-fn v) v)))
        (.append sb "\\N")))
    (.append sb \newline)
    (.toString sb)))

(defn escape-copy-text [^StringBuilder sb ^String v]
  (dotimes [i (.length v)]
    (case (.charAt v i)
      \\       (.append sb "\\\\")
      \tab     (.append sb "\\t")
      \newline (.append sb "\\n")
      \return  (.append sb "\\r")
      \b       (.append sb "\\b")     ; backspace
      \f       (.append sb "\\f")     ; form feed
      c        (.append sb c))))
```

### 8.5 File Encoding Load File Syntax

The existing pgloader encoding keyword is preserved:

```
LOAD CSV
  FROM '/path/file.csv'
  WITH ENCODING latin1
  ...
```

Maps to: `(java.nio.charset.Charset/forName "latin1")`

The `--list-encodings` flag from v3 is **dropped** in v4. Java's `Charset.availableCharsets()` is the canonical source.

---

## 9. v4 Prefetch Buffering

### 9.1 Architecture

A bounded producer-consumer pipeline decouples source reading from PostgreSQL writing:

```
┌──────────────┐    ┌────────────────┐    ┌───────────────┐
│ Reader Thread│───▶│ BlockingQueue  │───▶│ Writer Thread │
│ (source rows)│    │  of Batch      │    │ (CopyManager) │
│              │    │  capacity=4    │    │               │
└──────────────┘    └────────────────┘    └───────────────┘
                      ↑ back-pressure
                        blocks reader when queue full
```

### 9.2 Queue Structure

```clojure
(defrecord CopyPipeline
  [^BlockingQueue queue     ; LinkedBlockingQueue (capacity 4)
   ^long batch-rows         ; target rows per batch
   ^long batch-bytes        ; target bytes per batch
   ^AtomicBoolean done      ; signaled when reader finishes
   ^AtomicLong error-count] ; shared across reader/writer
```

**Queue capacity**: 4 batches. At ~25,000 rows × ~200 bytes = ~5 MB per batch, peak queue memory is ~20 MB. This is fixed and predictable — no sizing heuristics needed.

### 9.3 Reader Virtual Thread

```clojure
(defn reader-task
  [source table-spec ^CopyPipeline pipeline cast-specs]
  (let [max-rows  (.batch-rows pipeline)
        max-bytes (.batch-bytes pipeline)]
    (try
      (loop [batch (make-batch max-rows max-bytes)
             rows  (read-rows source table-spec)]
        (if-let [row (first rows)]
          (let [row-bytes (.getBytes (format-row row cast-specs)
                                     StandardCharsets/UTF_8)]
            (if (batch-full? batch)
              (do (.put (.queue pipeline) batch)   ; blocks if queue at capacity
                  (recur (make-batch max-rows max-bytes)
                         (rest rows)))
              (do (batch-add-row! batch row-bytes)
                  (recur batch (rest rows)))))
          (do (.put (.queue pipeline) batch)       ; final partial batch
              (.put (.queue pipeline) :end-of-data) ; sentinel
              (.set (.done pipeline) true)))))
      (catch Exception e
        (.set (.done pipeline) true)
        (throw e)))))
```

### 9.4 Writer Virtual Thread

```clojure
(defn writer-task
  [^PGConnection pg-conn table-spec ^CopyPipeline pipeline]
  (let [cm (.getCopyAPI pg-conn)]
    (loop [errors 0]
      (let [item (.take (.queue pipeline))]  ; blocks if empty
        (if (= :end-of-data item)
          (do (.commit pg-conn) errors)
          (let [^Batch batch item]
            (.begin pg-conn)
            (try
              (send-batch! pg-conn batch table-spec)
              (.commit pg-conn)
              (recur errors)
              (catch PSQLException e
                (.rollback pg-conn)
                (let [bad (retry-batch! batch table-spec e pg-conn)]
                  (recur (+ errors bad)))))))))))
```

### 9.5 Partitioned Reads

For tables with a monotonically increasing primary key, the table is split into row ranges, each with its own reader+writer pair:

```clojure
(defn partition-ranges
  "Split a table into row ranges for parallel reading.
   Uses the source's metadata to find min/max of the primary key."
  [^Source source table-spec]
  (let [{:keys [min-id max-id range-size]} (estimate-ranges source table-spec)]
    (for [start (range min-id max-id range-size)]
      (assoc table-spec :row-range [start (min (+ start range-size) max-id)]))))
```

Each range spawns its own `reader-task` + `writer-task` pair with a dedicated `CopyPipeline`.

### 9.6 Thread Coordination

```clojure
(defn copy-table!
  [source table-spec ^PGConnection pg-conn cast-specs]
  (let [pipeline (CopyPipeline. (LinkedBlockingQueue. 4)
                                *batch-rows* *batch-bytes*
                                (AtomicBoolean. false)
                                (AtomicLong. 0))
        reader   (Thread/startVirtualThread
                   #(reader-task source table-spec pipeline cast-specs))
        writer   (Thread/startVirtualThread
                   #(writer-task pg-conn table-spec pipeline))]
    (.join reader)
    (.join writer)))
```

---

## 10. Phase 1 Specification — PROTOTYPE

Goal: working end-to-end tool that:
1. Parses a `.load` file (CSV + MySQL subset of pgloader DSL)
2. Copies CSV files and MySQL databases to PostgreSQL
3. Passes all unit + integration tests

### 10.1 Modified Timeline (v2.0 changes from v1.1)

The previous spec had `src/pgloader/copy.clj` as a single monolithic writer. Split it into:
- `batch.clj` — Batch record, fill, send, retry-with-binary-search
- `prefetch.clj` — reader/writer pipeline, queue management
- `reject.clj` — reject file I/O
- `copy.clj` — COPY SQL generation, format-row, escape-copy-text (pure formatting)

All other Phase 1 modules remain as specified in v1.1 Section 7.

### 10.2 CLI Interface

Same as v1.1 Section 7.1. The `--workers` flag controls how many partitioned reader+writer pairs to use for large tables.

### 10.3 Load File Parser

Same as v1.1 Section 7.2 and Section 6 of this document. Phase 1 supports CSV loads and MySQL database loads.

### 10.4 Cast Registry

Same as v1.1 Section 7.4. Add to the registry:

```clojure
(def registry
  {:zero-dates-to-null      zero-dates-to-null
   :tinyint-to-boolean      tinyint-to-boolean
   :tinyint-to-integer      tinyint-to-integer
   :year-to-integer         year-to-integer
   :bytes-to-pg-bytea       bytes-to-pg-bytea
   :int-to-ip               int-to-ip
   :empty-string-to-null    empty-string-to-null
   :right-trim              (fn [^String v] (when v (clojure.string/trimr v)))
   :remove-null-characters  (fn [^String v] (when v (clojure.string/replace v "\0" "")))
   :none                    identity})
```

### 10.5 MySQL Source

See Section 5 (SQL files) and v1.1 Section 7.5. The MySQL source:
- Uses HugSQL functions from `resources/sql/mysql/` for catalog queries
- Opens one JDBC connection per table reader
- Sets `.setFetchSize(Integer/MIN_VALUE)` on data queries for streaming
- Reports column metadata (type, nullable, default, extra) for schema inference

### 10.6 CSV Source

```clojure
(ns pgloader.source.csv
  (:import [com.opencsv CSVReaderBuilder CSVParserBuilder]
           [java.io FileReader]
           [java.nio.charset Charset]))

(defn make-csv-source
  [{:keys [path delimiter quote-char escape-char skip-lines encoding
           columns target-schema target-table]}]
  (let [reader-fn (fn []
                    (let [parser (.. (CSVParserBuilder.)
                                     (withSeparator (first delimiter))
                                     (withQuoteChar (first quote-char))
                                     (withEscapeChar (first escape-char))
                                     (build))
                          fr (java.io.FileReader. path (Charset/forName (or encoding "UTF-8")))]
                      (.. (CSVReaderBuilder. fr)
                          (withCSVParser parser)
                          (withSkipLines (or skip-lines 0))
                          (build))))]
    (reify Source
      (source-name [_] (str "CSV: " path))
      (catalog [_]
        [{:source-table nil
          :target-schema (or target-schema "public")
          :target-table target-table
          :columns (mapv (fn [c] {:name c :source-type "text" :target-type "text"
                                  :cast nil :nullable true})
                         columns)}])
      (read-rows [_ table-spec]
        (let [reader (reader-fn)]
          (clojure.core/take-while
            some?
            (repeatedly #(.readNext reader)))))
      (close! [_]))))
```

### 10.7 DDL Generation

```clojure
(ns pgloader.ddl.common
  (:require [honey.sql :as sql]))

(def type-mapping
  {"bigint"       "bigint"
   "int"          "integer"
   "smallint"     "smallint"
   "tinyint"      "smallint"
   "varchar"      "text"
   "char"         "text"
   "text"         "text"
   "datetime"     "timestamptz"
   "timestamp"    "timestamptz"
   "date"         "date"
   "float"        "double precision"
   "double"       "double precision"
   "decimal"      "numeric"
   "blob"         "bytea"
   "binary"       "bytea"
   "boolean"      "boolean"
   "bit"          "boolean"
   ;; Extend per source-type in ddl/mysql.clj, ddl/mssql.clj, etc.
   })

(defn create-table-sql [table-spec]
  (let [cols (mapv (fn [c]
                     [(keyword (:name c))
                      (get type-mapping (:source-type c) "text")
                      (when-not (:nullable c) [:not :null])])
                   (:columns table-spec))]
    (sql/format {:create-table [(keyword (:target-schema table-spec)
                                         (:target-table table-spec))]
                 :with-columns cols})))
```

### 10.8 Integration Tests

Same as v1.1 Section 8. The `docker-compose.yml` provides MySQL 8.0 and PostgreSQL 16. Integration tests:
1. Start services
2. Seed MySQL with a test schema
3. Run pgloader with a `.load` file
4. Verify data in PostgreSQL matches source

---

## 11. Full Phase Plan

**Phase 1** — CSV + MySQL prototype with load file parser and batch error handling
- Everything in Section 10 of this document
- Includes: batch binary search (`batch.clj`), prefetch queue (`prefetch.clj`), reject files (`reject.clj`), encoding strategy
- Includes: load file parser + all parser tests
- Includes: integration tests for CSV and MySQL

**Phase 2** — Schema DDL execution + indexes + sequences
- `CREATE TABLE` execution before data load
- `CREATE INDEX` after table data (one virtual thread per index)
- `ALTER TABLE ADD FOREIGN KEY` after all tables
- `RESET SEQUENCE` for auto-increment columns

**Phase 3** — Additional JDBC sources
- SQLite (`source/sqlite.clj`, `ddl/sqlite.clj`, `resources/sql/sqlite/`)
- MSSQL (`source/mssql.clj`, `ddl/mssql.clj`, `resources/sql/mssql/`)
- PostgreSQL→PostgreSQL (`source/postgresql.clj`)

**Phase 4** — Full load file DSL compatibility
- Syntax additions: `ALTER TABLE`, `ALTER SCHEMA`, `BEFORE/AFTER LOAD EXECUTE`, `MATERIALIZE VIEWS`, `INCLUDING/EXCLUDING TABLE NAMES MATCHING`, `schema-only`, `data-only`, `truncate`, `disable triggers`
- Must-complete-before-closing: parse every `.load` file in `../test/` (the existing pgloader test suite) without error

**Phase 5** — File format sources
- DBF (`source/dbf.clj`, `com.github.albfernandez/javadbf`)
- Fixed-width (`source/fixed.clj`)
- Archive support (zip/tar/gzip)
- HTTP(S) URL source

**IXF explicitly excluded** from scope. No Java library exists; format is rare.

**Phase 6** — Oracle + GraalVM native binary
- Oracle via `gvenzl/oracle-free` Docker image (opt-in `--profile oracle`)
- GraalVM native-image build
- Tracing agent run for all source types
- CI matrix: Linux amd64, Linux arm64, macOS arm64, Windows amd64

---

## 12. Key Parameters (from v3, ported to v4)

| Parameter | Default | Description |
|---|---|---|
| `*batch-rows*` | 25,000 | Target rows per COPY batch (actual is randomized 0.7x–1.3x) |
| `*batch-size*` | 20 MiB | Max bytes per COPY batch |
| `*prefetch-queue-capacity*` | 4 | Number of batches in the prefetch queue |
| `*rows-per-range*` | 10,000 | Rows per partitioned reader range |
| `*on-error-stop*` | false | If true, abort on first error instead of continuing |
| `*root-dir*` | `/tmp/pgloader/` | Directory for reject files and logs |
| `*identifier-case*` | `:downcase` | How to handle identifier casing (`:downcase`, `:quote`, `:snake_case`) |

---

## 13. File Format Library Reference

Same as v1.1 Section 11. OpenCSV for CSV, JavaDBF for DBF (Phase 5).

---

## 14. Oracle in Docker

Same as v1.1 Section 10. Use `gvenzl/oracle-free:23-faststart`. Opt-in via Docker Compose profiles.

---

## 15. GraalVM Build Specification

Same as v1.1 Section 12. Run tracing agent for every source type before building native image.

---

## 16. Constraints for Claude Code

1. **Work in `clojure/` subdirectory** on branch `v4-clojure-rewrite`. Never modify files outside `clojure/`.
2. **Start with Phase 1 only.** Phases 2–6 are deferred until Phase 1 tests pass.
3. **Batch error handling is Phase 1.** `batch.clj` with `retry-batch!` binary search is not deferred.
4. **Encoding and prefetch are Phase 1.** Designed in Sections 8–9, implemented in Phase 1.
5. **SQL files in `resources/sql/`, never inline.** HugSQL loader.
6. **No `proxy`** — use `reify`. **No `eval`**. **`*warn-on-reflection* true`** everywhere.
7. **CopyManager is the only write path.** No `jdbc/execute!` for data.
8. **HugSQL, not YeSQL.**
9. **IXF out of scope.**
10. **Oracle is Phase 6.**

# pgloader — Agent Guide

This document explains the repository structure, the in-progress Clojure
rewrite, how to run tests for both implementations, and the conventions an
agent should follow when working in this codebase.

---

## Repository layout

```
pgloader/
├── src/                    # Common Lisp implementation (production)
│   ├── main.lisp           # Entry point, CLI parsing
│   ├── api.lisp            # High-level load-data / run-commands API
│   ├── params.lisp         # Global parameters (*log-filename* etc.)
│   ├── sources/            # One sub-directory per source type
│   │   ├── mysql/          # MySQL/MariaDB introspection + row reading
│   │   ├── sqlite/         # SQLite PRAGMA-based introspection
│   │   ├── pgsql/          # PostgreSQL→PostgreSQL migration
│   │   ├── mssql/          # SQL Server (FreeTDS / ODBC-based)
│   │   ├── db3/            # dBASE III / DBF files
│   │   ├── csv.lisp        # CSV file reader
│   │   ├── copy.lisp       # PostgreSQL COPY TEXT format reader
│   │   └── ...
│   ├── pgsql/              # PostgreSQL target: DDL, COPY, schema management
│   ├── pg-copy/            # Batch sending, retry, reject file logic
│   ├── load/               # Orchestration: migrate-database, copy-data
│   ├── utils/              # Logging, monitor, state, stats, report
│   └── parsers/            # LOAD FILE DSL parsers (per source type)
├── test/                   # CL integration test data and load files
│   ├── data/               # CSV, DBF, COPY format fixture files
│   ├── sqlite/             # SQLite .db files (Chinook, testpk)
│   ├── *.load              # LOAD FILE commands for each test scenario
│   └── regress/expected/   # Expected query output for regression tests
├── clojure/                # Clojure v4 rewrite (in progress — Phase 1 done)
│   ├── src/pgloader/       # Clojure source
│   ├── test/               # Clojure tests
│   ├── test/fixtures/      # Test data (CSV, SQLite, DBF, COPY files)
│   ├── IMPLEMENTATION-PLAN.md   # Current work tracker
│   ├── prompt-*.md         # Detailed prompts for OpenCode on specific tasks
│   └── AGENT.md → (this file is at repo root)
├── pgloader.asd            # ASDF system definition for CL build
└── Makefile                # Top-level CL build and test targets
```

---

## Common Lisp implementation

### Architecture

The CL codebase is a classic pipeline:

1. **Parse** — `src/parsers/` transforms a LOAD FILE into a command object
   (source connection, target connection, options, cast rules, filters).

2. **Fetch catalog** — `fetch-metadata` on the source introspects its schema
   (tables, columns, indexes, FKs) and produces a `catalog` structure.

3. **Prepare target** — `prepare-pgsql-database` creates schemas, types,
   tables in one transaction. If it fails, the whole migration aborts.

4. **Copy data** — `copy-database` iterates tables, spawning lparallel worker
   threads. Each table gets a reader thread and one or more writer threads
   connected by an lparallel queue. Writers send batches via
   `cl-postgres:open-db-writer` / `close-db-writer` (the PostgreSQL COPY
   protocol). On data errors, `retry-batch` does binary search to isolate
   bad rows.

5. **Post-load** — `complete-pgsql-database` creates indexes, foreign keys,
   resets sequences, runs AFTER LOAD DO SQL.

6. **Summary** — the monitor thread accumulates per-table stats (rows, errors,
   bytes, timing) and prints a formatted table at the end.

Key source files:
- `src/load/migrate-database.lisp` — top-level orchestration
- `src/load/copy-data.lisp` — per-table reader/writer thread setup
- `src/pg-copy/copy-rows-in-batch.lisp` — batch send + error recovery
- `src/pg-copy/copy-retry-batch.lisp` — binary search retry logic
- `src/utils/monitor.lisp` — statistics collection via lparallel queue
- `src/utils/pretty-print-state.lisp` — summary table formatting

### Running the CL tests

The CL test suite uses a Makefile inside `test/` and a docker-compose file for
each source type. Tests always need a live PostgreSQL target.

```bash
# Start PostgreSQL (the CL tests talk to localhost:5432 by default)
docker compose up -d postgres    # or use your local PostgreSQL

# Run all regression tests
make                             # in the repo root

# Run a specific load scenario
pgloader test/csv.load
pgloader test/sqlite-chinook.load
```

The regression tests compare `SELECT * FROM table ORDER BY pk` output against
the expected files in `test/regress/expected/*.out`.

---

## Clojure v4 rewrite

### Status

Phase 1 is complete: MySQL/MariaDB and CSV sources, post-load DDL (indexes,
FKs, sequences), full transaction safety, INFO-level log narrative, summary
table. See `clojure/IMPLEMENTATION-PLAN.md` for the detailed work log.

Phase 2 in progress: COPY format, SQLite, DBF, PostgreSQL-as-source, MSSQL.
See `clojure/prompt-new-sources.md` for the step-by-step implementation plan.

### Architecture

```
cli.clj          CLI parsing, logging configuration, entry point
core.clj         Orchestration: DDL phase → COPY phase → post-load DDL
                 Manages the single PostgreSQL connection (autoCommit=false)
source/
  protocol.clj   Source protocol: source-name, catalog, read-rows, close!
  mysql.clj      MySQL/MariaDB — HugSQL catalog, JDBC streaming read
  csv.clj        CSV — OpenCSV, inline data, glob patterns, projections
  copy.clj       COPY TEXT format files (to implement)
  sqlite.clj     SQLite — PRAGMA catalog, sqlite-jdbc (to implement)
  dbf.clj        DBF/dBASE files — javadbf (to implement)
  pgsql.clj      PostgreSQL→PostgreSQL (to implement)
  mssql.clj      SQL Server — mssql-jdbc (to implement)
prefetch.clj     Reader/writer virtual thread pipeline, LinkedBlockingQueue
batch.clj        Batch accumulation, send-batch!, retry-loop (binary search)
copy.clj         COPY TEXT encoding (tab escape, backslash escape, NULL=\N)
reject.clj       Per-table reject files (.dat raw data, .log error messages)
ddl/common.clj   SQL generators: CREATE TABLE, CREATE INDEX, ADD FK, setval
load_file/
  grammar.clj    Instaparse grammar for the LOAD DSL
  ast.clj        Parse tree → LoadCommand record
  parser.clj     Instaparse wrapper
stats.clj        Phase-based stats (pre/data/post), per-table tracking
summary.clj      ASCII summary table + CSV/JSON export
log.clj          fmt-duration, fmt-bytes helpers
```

### Transaction model

The PostgreSQL connection is created with `autoCommit=false` and never toggled.
All operations commit or rollback explicitly:

- **DDL per table** — `run-ddl-tx`: executes DROP TABLE + CREATE TABLE + CREATE
  SCHEMA in one transaction; on failure rolls back (leaving existing table
  intact).
- **BEFORE/AFTER LOAD DO** — all statements in one transaction.
- **SET parameters** — each SET in its own commit (session-scoped).
- **COPY batches** — each batch in its own transaction via `.commit()` /
  `.rollback()` in `prefetch.clj/send-batch-or-retry!`.
- **Retry sub-batches** — each sub-batch in `batch.clj/retry-loop` commits
  independently, matching the CL `copy-partial-batch` function.
- **Post-load DDL** — each index/FK statement in its own commit;
  failures are logged and skipped (non-fatal).

### Running the Clojure tests

#### Prerequisites

- JDK 21+
- Clojure CLI 1.12+
- Docker + Docker Compose v2
- Run `make check-env` in `clojure/` to verify.

#### Unit tests (no database needed)

```bash
cd clojure
clojure -M:test -n pgloader.cast-test
clojure -M:test -n pgloader.batch-test
clojure -M:test -n pgloader.ddl-test
clojure -M:test -n pgloader.load-file.parser-test
clojure -M:test -n pgloader.summary-test

# Or via make
make test-unit
```

#### Integration tests — CSV, COPY format, SQLite, DBF

These need PostgreSQL only. The base `docker-compose.yml` provides it:

```bash
cd clojure
docker compose up -d postgres
# wait for postgres to be healthy, then:
clojure -M:test -n pgloader.integration.csv-pg-test
clojure -M:test -n pgloader.integration.copy-pg-test
clojure -M:test -n pgloader.integration.sqlite-pg-test
clojure -M:test -n pgloader.integration.dbf-pg-test
docker compose down
```

The pgloader test process connects to `localhost:5432` (postgres container's
published port). Test fixtures are in `test/fixtures/` and referenced by
relative path from the `clojure/` working directory.

#### Integration tests — MySQL/MariaDB

```bash
cd clojure
make test-integration    # uses docker-compose.yml (basic seed)
make test-datasets        # uses test-mysql-compose.yml (sakila + f1db)
make test-datasets-mariadb
```

Each `make test-*` target starts the required services, runs the tests, and
tears down.

#### Integration tests — PostgreSQL as source

```bash
cd clojure
docker compose -f test-pgsql-compose.yml up -d
clojure -M:test -n pgloader.integration.pgsql-pg-test
docker compose -f test-pgsql-compose.yml down
```

#### Integration tests — MSSQL

```bash
cd clojure
docker compose -f test-mssql-compose.yml up -d
# Wait ~60s for SQL Server to initialise, then:
clojure -M:test -n pgloader.integration.mssql-pg-test
docker compose -f test-mssql-compose.yml down
```

SQL Server startup takes 30–60 seconds. The compose healthcheck retries 20
times with 10s intervals. The `mssql-init` service runs the seed script after
the healthcheck passes.

#### Full end-to-end (docker compose run pgloader)

The `test-mysql-compose.yml` and `test-mariadb-compose.yml` include a
`pgloader` service that builds the JAR, runs it inside a container, and
executes the load scenarios against the bundled databases. This is the
canonical integration path:

```bash
cd clojure
docker compose -f test-mariadb-compose.yml up pgloader
```

This runs `make f1db sakila my db789` inside the container, exercising the
full stack.

---

## SQL catalog query reuse pattern

### From CL to Clojure

The CL sources use SQL files with CL `format` parameters (`~a`, `~:[~*~;...]`).
The Clojure rewrite uses HugSQL (for JDBC-parameterised sources) or plain
format strings (for PRAGMA-based sources like SQLite).

**HugSQL pattern** (MySQL example):

```
CL file:    src/sources/mysql/list-all-indexes.sql  (uses ~a placeholders)
Clojure:    src/pgloader/source/mysql.sql            (uses :schema, :table)
Load with:  (hugsql/def-db-fns "pgloader/source/mysql.sql")
Call with:  (table-indexes conn {:schema "mydb" :table "orders"})
```

When porting a CL SQL file:
1. Replace `~a` with the appropriate `:param-name`.
2. Remove the `~:[~*~;and (...)]` dynamic filter clauses (implement
   INCLUDING/EXCLUDING as Clojure post-filters for now).
3. Verify the column aliases match what the Clojure source code expects
   (e.g., `AS column_name`, `AS table_name`).

**PRAGMA pattern** (SQLite):

```clojure
;; PRAGMA table_info can't use ? parameters — table name is substituted directly
(jdbc/execute! conn [(str "PRAGMA table_info(`" table-name "`)")])
```

### Catalog output contract

Every `catalog` method must return a vector of maps with this exact shape:

```clojure
[{:table-name  "orders"          ; string, no schema prefix
  :schema      "public"          ; string
  :columns     [{:column-name    "id"
                 :column-type    "integer"   ; PostgreSQL target type
                 :is-nullable    false
                 :column-default nil         ; or "nextval(...)", "0", etc.
                 :extra          "auto_increment"  ; or nil
                 :column-key     "PRI"}]     ; optional, used for index hints
  :primary-key ["id"]            ; vector of column name strings
  :indexes     [{:name     "idx_orders_customer"
                 :unique   false
                 :columns  "customer_id"}]  ; columns as comma-joined string
  :fkeys       [{:name      "fk_orders_customer"
                 :columns   "customer_id"
                 :ftable    "customers"
                 :fcols     "id"
                 :on-delete "NO ACTION"
                 :on-update "NO ACTION"}]}]
```

The DDL generator in `ddl/common.clj` reads this structure. Keep the keys
consistent — adding extra keys is fine, removing or renaming existing ones
breaks DDL generation.

---

## Log format and summary target

### Log format

The Clojure rewrite uses Logback via `clojure.tools.logging`. The logback
config is in `resources/logback.xml`. Console output at INFO level should
narrate the migration as an operator would expect:

```
12:06:12.722 INFO  pgloader.core  - pgloader v4 POC
12:06:12.722 INFO  pgloader.core  - Source: mysql://f1db/
12:06:12.722 INFO  pgloader.core  - Target: postgresql://...@postgres:5432/target
12:06:12.745 INFO  pgloader.core  - Connected to PostgreSQL
12:06:12.758 INFO  pgloader.core  - Found 14 tables in source catalog
12:06:12.759 INFO  pgloader.core  - Processing tables in this order: circuits, ...
12:06:12.760 INFO  pgloader.core  - COPY circuits [1/14]
12:06:12.779 INFO  pgloader.core  - COPY circuits done: 77 rows in 0.013s
...
12:06:13.804 INFO  pgloader.core  - Done copying all tables
```

Use `log/debug` for DDL SQL text, per-batch detail, retry events.
Use `log/warn` for rejected rows, encoding coercions, skipped constraints.
Use `log/error` for table-load failures (with the exception).

The CL log level mapping: `:log` / `:notice` → `log/info`, `:info` →
`log/info`, `:debug` → `log/debug`, `:warning` → `log/warn`,
`:error` / `:fatal` → `log/error`.

### Summary table format

The summary table at the end of a run matches the CL output structure. The
`summary/print-summary` function in `summary.clj` renders three sections
(Before LOAD / Data / After LOAD) when non-empty:

```
Results:

Data:
table name             │ errors │     rows │      bytes │ total time
────────────────────────┼────────┼──────────┼────────────┼────────────
circuits               │      0 │       77 │    9.11 kB │      0.013s
constructorresults     │      0 │    12625 │  238.76 kB │      0.058s
...
────────────────────────┼────────┼──────────┼────────────┼────────────
Total import time:     │      ✓ │   701408 │   19.72 MB │      1.010s
```

Key formatting rules:
- Individual table rows show integer error counts (`0` not `✓`).
- The grand-total line shows `✓` for zero total errors.
- Time is always `%.3fs` format (never `ms` suffix, no zero-padding like
  `01.010s`).
- Column widths are computed dynamically from the longest label in the run.

---

## When adding a new source type

Checklist:

- [ ] Create `src/pgloader/source/<name>.clj` implementing `Source` protocol
- [ ] Create `src/pgloader/source/<name>.sql` (HugSQL) if using JDBC catalog
- [ ] Update `grammar.clj` if a new URI scheme or FROM syntax is needed
- [ ] Update `ast.clj` to handle the new grammar nodes and URI scheme
- [ ] Add dispatch to `source-from-uri` in `core.clj`
- [ ] Copy test fixture files to `test/fixtures/<name>/`
- [ ] Create docker-compose file if a source server is needed
- [ ] Write integration tests in `test/pgloader/integration/<name>_pg_test.clj`
- [ ] Run `make test-unit` — must pass with zero failures
- [ ] Run the integration test suite — must pass
- [ ] Check that the summary output matches the expected format

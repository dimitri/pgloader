# pgloader v4 (Clojure)

Clojure-based reimplementation of pgloader, a data loading tool for PostgreSQL.
Supports MySQL, MariaDB, SQLite, CSV, COPY, DBF, Fixed-width, MSSQL, and
PostgreSQL-as-source migration.

## Status

This is an **in-progress** rewrite. Core migration paths (CSV, MySQL, MariaDB,
SQLite, DBF, PostgreSQL) are fully implemented and covered by integration tests
that run against both v3 and v4 in CI. Known gaps:

- **MSSQL**: source introspection is implemented; full end-to-end coverage is limited.
- **IXF / Redshift**: not planned for v4 at this time.
- **Built-in transforms**: all 19 v3 transforms are ported to `pgloader.transforms`.

## Intentional differences from v3

### INI config files — not supported

v3 supported two INI-related features that are **not implemented in v4** and
will not be:

1. **`--upgrade-config FILE.conf`** — converted pgloader v2.x INI-format
   configuration files to v3 load file syntax. pgloader v2 has been
   unsupported for many years; this migration path is no longer needed.

2. **`--context FILE.ini`** — fed an INI file as a mustache template context
   into a `.load` file (e.g. `{{DBPATH}}`). v4 replaces this with standard
   OS environment variables: set `DBPATH` in the shell environment and
   reference it in the load file as `$DBPATH` or via `${DBPATH}`.

If you relied on `--context`, move your INI values to environment variables
and reference them in your `.load` file. No load-file syntax changes are
needed for simple key=value substitution.

## Directory structure

```
clojure/
├── build.clj                         — Uberjar build config (tools.build)
├── deps.edn                          — Clojure CLI dependencies & aliases
├── Dockerfile                        — Multi-stage Docker build (JDK 21)
├── Makefile                          — Dev workflow: test, build, run
├── resources/                        — JVM resources (logback.xml, etc.)
├── src/pgloader/
│   ├── core.clj                      — Entry point, migration orchestrator
│   ├── cli.clj                       — Command-line argument parsing
│   ├── cast.clj                      — Type casting framework
│   ├── copy.clj                      — PostgreSQL COPY format encoding
│   ├── batch.clj                     — Row batch accumulation for COPY
│   ├── prefetch.clj                  — Concurrent reader/writer pipeline
│   ├── reject.clj                    — Rejected row handling
│   ├── transforms.clj                — Built-in column transform functions
│   ├── stats.clj / summary.clj       — Progress stats and summary table
│   ├── regress.clj                   — pg_regress-style test driver
│   ├── load_file/                    — LOAD command file parser
│   │   ├── grammar.clj               — Instaparse grammar definition
│   │   ├── parser.clj                — Parser wrapper & entry point
│   │   └── ast.clj                   — AST transform → LoadCommand record
│   ├── ddl/                          — DDL generation (CREATE TABLE, etc.)
│   │   ├── common.clj                — Shared DDL utilities
│   │   ├── mysql.clj                 — MySQL → PostgreSQL DDL mapping
│   │   └── citus.clj                 — Citus distribution DDL + FK discovery
│   └── source/                       — Data source implementations
│       ├── protocol.clj              — Source protocol definition
│       ├── csv.clj                   — CSV file source
│       ├── copy.clj                  — PostgreSQL COPY text format source
│       ├── fixed.clj                 — Fixed-width file source
│       ├── dbf.clj                   — dBASE III/IV (DBF) source
│       ├── sqlite.clj                — SQLite source
│       ├── mysql.clj                 — MySQL/MariaDB source
│       ├── pgsql.clj                 — PostgreSQL-as-source
│       └── mssql.clj                 — SQL Server source
├── test/                             — Unit test namespaces
│   └── pgloader/
│       ├── cast_test.clj
│       ├── batch_test.clj
│       ├── ddl_test.clj
│       └── load_file/parser_test.clj
└── tests/                            — Integration test suites (Docker Compose)
    ├── Makefile                      — Top-level: csv, dbf, sqlite, mysql, pgsql, …
    ├── csv/                          — CSV + COPY + Fixed-width tests
    ├── dbf/                          — DBF (dBASE) tests
    ├── sqlite/                       — SQLite tests
    ├── mysql/                        — MySQL 8.0 tests (sakila, f1db, my)
    ├── mysql57/                      — MySQL 5.7 tests
    ├── mariadb/                      — MariaDB tests
    ├── pgsql/                        — PostgreSQL → PostgreSQL tests
    ├── mssql/                        — SQL Server tests
    ├── citus/                        — Citus distributed table tests
    └── file-based/                   — CSV / COPY / Fixed / DBF / SQLite test data
```

## Requirements

- JDK 21+
- Clojure CLI 1.11+
- Docker + Docker Compose v2 (for integration tests)

## Build

```sh
clojure -T:build uber       # produces target/pgloader-v4-*.jar
make build                  # same, via Makefile
```

## How to run tests

All integration tests run entirely inside Docker Compose — no local database
needed.

### Unit tests (no Docker)

```sh
make test-unit
# or directly:
clojure -M:test -m cognitect.test-runner \
  -n pgloader.cast-test -n pgloader.batch-test \
  -n pgloader.ddl-test -n pgloader.load-file.parser-test
```

### Integration tests (Docker Compose)

```sh
# From clojure/tests/:
make csv           # CSV + COPY + Fixed-width (v4)
make csv-v3        # same tests with pgloader v3
make mysql         # MySQL 8.0 → PostgreSQL
make sqlite        # SQLite → PostgreSQL
make dbf           # DBF → PostgreSQL
make pgsql         # PostgreSQL → PostgreSQL
make mssql         # SQL Server → PostgreSQL
make citus         # Citus distributed tables
make all           # all suites with v4
make all-v3        # all suites with v3

# Regenerate expected baselines (uses v3 as source of truth):
make update-expected-csv
make update-expected-mysql
```

Each target spins up the required database services, waits for health-checks,
runs tests inside the `test-runner` container, then cleans up.

## Run migrations

```sh
# From LOAD file (after build):
java -jar target/pgloader-v4-*.jar path/to/file.load

# Direct URI migration:
java -jar target/pgloader-v4-*.jar mysql://user:pass@host/db pgsql://user:pass@host/db

# Pipe data via stdin:
cat data.csv | java -jar target/pgloader-v4-*.jar /path/to/file.load
```

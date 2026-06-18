# pgloader v4 — integration test suites

This directory contains all E2E integration tests. Each suite is a self-contained
directory with its own `docker-compose.yml`, a `Makefile`, and one subdirectory
per test case.

---

## Directory layout

```
tests/
├── csv/                 CSV files (41 tests, including stdin)
├── copy/                PostgreSQL COPY-format files (1 test)
├── fixed/               Fixed-width files (4 tests)
├── dbf/                 dBASE .dbf files (5 tests, including HTTP ZIP)
├── sqlite/              SQLite databases (5 tests)
├── mysql/               MySQL 8 → PostgreSQL (5 tests: basic, cast, filters, sakila, f1db)
├── mysql57/             MySQL 5.7 → PostgreSQL (canonical CL parity suite)
├── mariadb/             MariaDB 11 → PostgreSQL (3 tests: basic, sakila, f1db)
├── pgsql/               PostgreSQL → PostgreSQL (1 test)
├── mssql/               MS SQL Server → PostgreSQL (1 test)
└── citus/               MySQL → Citus distributed PostgreSQL (1 test)
```

Each suite directory is self-contained: its `docker-compose.yml` declares all
services, and its `Makefile` runs all tests.  The CSV and DBF suites include a
static fileserver for HTTP fixture tests; other file-based suites (copy, fixed,
sqlite) only need PostgreSQL.

Each test directory contains:

```
<test-name>/
├── <test-name>.load     pgloader command file
├── sql/
│   └── <test-name>.sql  psql queries run after the load (one or more .sql files)
├── expected/
│   └── <test-name>.out  committed baseline output (what psql should produce)
└── out/                 runtime output — gitignored, created during test runs
    └── <test-name>.out
```

---

## How tests work

Each suite `Makefile` repeats the same pattern for every test:

```makefile
basic:
	java -jar $(JAR) /work/basic/basic.load      # 1. run the load
	java -jar $(JAR) regress $(REGRESS_FLAGS) /work/basic  # 2. compare output
```

The `regress` sub-command (implemented in `pgloader.regress`):

1. Finds every `*.sql` file under `<test>/sql/`.
2. Runs each through `psql -X -P pager=off -v ON_ERROR_STOP=1 -f <file>`,
   capturing combined stdout+stderr to `<test>/out/<stem>.out`.
3. Diffs `out/<stem>.out` against `expected/<stem>.out`.
4. Exits non-zero if any diff is non-empty or any baseline file is missing.

With `--update` (`REGRESS_FLAGS=--update`), step 3 writes `out/` → `expected/`
instead of diffing — this registers or refreshes the baseline.

---

## Prerequisites

```sh
# From the clojure/ directory:
make build              # compile the uberjar  →  target/pgloader.jar
```

The MySQL and MariaDB suites also need the F1DB dataset (~23 MB, CC BY-SA 4.0):

```sh
make -C tests ensure-f1db   # downloads test/data/f1db.sql if absent
```

---

## Running tests

### Run a single suite

```sh
# From the clojure/ directory:
make -C tests csv
make -C tests copy
make -C tests fixed
make -C tests dbf
make -C tests sqlite
make -C tests mysql
make -C tests mariadb
make -C tests pgsql
make -C tests mssql
make -C tests citus
```

Each target starts the required Docker services, runs the suite Makefile inside
a `test-runner` container, then stops.

### Run all suites

```sh
make -C tests
# or from the clojure/ directory:
make tests
```

### Run unit tests (no Docker, no database)

The Clojure unit tests are fully self-contained — they require only the JDK
and the Clojure CLI. No Docker, no running PostgreSQL, no fixture downloads.
They run directly on the host and are the fastest feedback loop during
development.

```sh
make test-unit
# or directly, from the clojure/ directory:
clojure -M:test -m cognitect.test-runner \
  -n pgloader.cast-test \
  -n pgloader.batch-test \
  -n pgloader.ddl-test \
  -n pgloader.load-file.parser-test \
  -n pgloader.transforms-test \
  -n pgloader.pg-service-test \
  -n pgloader.cli-test
```

Test files live under `clojure/test/pgloader/`:

| Namespace | File | What it covers |
|---|---|---|
| `pgloader.cast-test` | `cast_test.clj` | Type cast rule resolution — column and type rules, precedence, `drop-typemod`, `using` transforms |
| `pgloader.batch-test` | `batch_test.clj` | Row batch accumulation, size/row-count limits, COPY-format byte serialisation |
| `pgloader.ddl-test` | `ddl_test.clj` | DDL generation — `CREATE TABLE`, `DROP TABLE IF EXISTS`, index and FK SQL, sequence resets, identifier quoting |
| `pgloader.load-file.parser-test` | `load_file/parser_test.clj` | Full round-trip parsing of `.load` files for every supported command type |
| `pgloader.transforms-test` | `transforms_test.clj` | Transform functions (`zero-dates-to-null`, `date-with-no-separator`, etc.) |
| `pgloader.pg-service-test` | `pg_service_test.clj` | `.pgpass` lookup and `pg_service.conf` parsing |
| `pgloader.cli-test` | `cli_test.clj` | CLI argument parsing — all flags, positional args, `--type`, `--with`, `--cast`, etc. |

---

## Registering or refreshing expected output

Expected output files live in `expected/` and are committed to the repository.
They must be regenerated whenever:

- a new test is added (no `expected/` file exists yet), or
- an intentional output change is made (schema evolution, column rename, etc.).

### All tests in a suite

```sh
make -C tests update-expected-csv
make -C tests update-expected-copy
make -C tests update-expected-fixed
make -C tests update-expected-dbf
make -C tests update-expected-sqlite
make -C tests update-expected-mysql
make -C tests update-expected-mariadb
make -C tests update-expected-pgsql
make -C tests update-expected-mssql
make -C tests update-expected-citus
```

This passes `REGRESS_FLAGS=--update` to the container, which writes
`out/<stem>.out` → `expected/<stem>.out` for every test in the suite.
Commit the resulting `expected/` files.

### A single test (inside a running container or with local psql)

```sh
# With a running postgres accessible via env vars:
java -jar target/pgloader.jar basic/basic.load
java -jar target/pgloader.jar regress --update basic
```

---

## Adding a new test

1. Create `tests/<suite>/<test-name>/` with:
   - `<test-name>.load` — the pgloader command file
   - `sql/<test-name>.sql` — one or more psql queries that verify the load
2. Add the target to the suite `Makefile` and its `.PHONY` and `all:` lists:
   ```makefile
   .PHONY: all … <test-name>
   all: … <test-name>

   <test-name>:
   	java -jar $(JAR) /work/<test-name>/<test-name>.load
   	java -jar $(JAR) regress $(REGRESS_FLAGS) /work/<test-name>
   ```
3. Register the baseline:
   ```sh
   make -C tests update-expected-<suite>
   ```
4. Commit `expected/<test-name>.out`.

---

## HTTP fixtures and static fileserver

Tests that load from HTTP URLs (`dbf/dbf-zip`, `fixed/census-places`) use a
static file server (`joseluisq/static-web-server:2`) declared in their
suite `docker-compose.yml`. The fixture ZIP files are committed to the
repository at `tests/fixtures/http/` so no network access is required during
test runs.

---

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `JAR` | `/pgloader.jar` | Path to the pgloader uberjar inside the container |
| `REGRESS_FLAGS` | _(empty)_ | Extra flags passed to `regress`; set to `--update` to write baselines |
| `PGHOST` | `postgres` | PostgreSQL host (set by docker-compose) |
| `PGPORT` | `5432` | PostgreSQL port |
| `PGDATABASE` | `target` | Target database name |
| `PGUSER` | `pgloader` | PostgreSQL user |
| `PGPASSWORD` | `pgloader` | PostgreSQL password |

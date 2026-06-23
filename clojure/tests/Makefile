# ─── pgloader E2E integration test suites ────────────────────────────────────
#
# Called as $(MAKE) -C tests from the clojure/ root Makefile,
# or run directly from clojure/tests/.
#
# Two modes for every suite target:
#   suitename      — load with pgloader v4 (Clojure jar), validate with SQL
#   suitename-v3   — load with pgloader v3 (Common Lisp), validate with same SQL
#
# Both modes use identical sql/*.sql validation queries — the only difference
# is which binary performs the data migration.
#
# Usage (from clojure/):
#   make -C tests                        — run all suites with v4
#   make -C tests all-v3                 — run all suites with v3
#   make -C tests mysql                  — mysql suite with v4
#   make -C tests mysql-v3               — mysql suite with v3
#   make -C tests update-expected-mysql  — regenerate baselines (loads with v3)
#   make -C tests down                   — stop all compose stacks
#
# Prerequisites:
#   - clojure/target/pgloader.jar (built with: clojure -T:build uber)
#   - Docker Desktop / Docker Engine with Compose v2

# ─── Variables ────────────────────────────────────────────────────────────────

ROOT     ?= ..
JAR      ?= $(ROOT)/target/pgloader.jar

DC       = docker compose
PROFILE  = --profile test

# ─── Top-level targets ────────────────────────────────────────────────────────

.PHONY: all all-v3
.PHONY: csv copy fixed dbf sqlite
.PHONY: csv-v3 copy-v3 fixed-v3 dbf-v3 sqlite-v3
.PHONY: update-expected-csv update-expected-copy update-expected-fixed
.PHONY: update-expected-dbf update-expected-sqlite
.PHONY: mysql mysql57 mariadb pgsql mssql citus
.PHONY: mysql-v3 mysql57-v3 mariadb-v3 pgsql-v3 mssql-v3 citus-v3
.PHONY: update-expected-mysql update-expected-mysql57
.PHONY: update-expected-mariadb update-expected-pgsql update-expected-mssql update-expected-citus
.PHONY: down ensure-jar ensure-f1db
.PHONY: bench employees employees-v3 lahman lahman-v3 bench-report bench-down

all: csv copy fixed dbf sqlite mysql mysql57 mariadb pgsql mssql citus

all-v3: csv-v3 copy-v3 fixed-v3 dbf-v3 sqlite-v3 mysql-v3 mysql57-v3 mariadb-v3 pgsql-v3 mssql-v3 citus-v3

# ─── Prerequisite checks ──────────────────────────────────────────────────────

ensure-jar:
	@test -f $(JAR) || { \
	  echo "ERROR: $(JAR) not found."; \
	  echo "  Build it first: clojure -T:build uber"; \
	  echo "  Then symlink:   ln -sf pgloader-v4-*.jar target/pgloader.jar"; \
	  exit 1; \
	}

ensure-f1db:
	@if [ ! -f $(ROOT)/../test/data/f1db.sql ]; then \
	  echo "=== F1DB dataset not found — downloading (CC BY-SA 4.0, ~23 MB)... ==="; \
	  bash $(ROOT)/../test/mysql/download-f1db.sh; \
	fi

# ─── Suite: csv ───────────────────────────────────────────────────────────────
# CSV (amalgamated single pgloader run) + COPY + Fixed-width.

csv: ensure-jar
	@$(DC) -f csv/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f csv/docker-compose.yml run --rm --build test-runner
	@$(DC) -f csv/docker-compose.yml down --volumes 2>/dev/null; true

csv-v3: ensure-jar
	@$(DC) -f csv/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f csv/docker-compose.yml run --rm --build \
	  -e "PGLOADER_CMD=pgloader --client-min-messages notice" test-runner
	@$(DC) -f csv/docker-compose.yml down --volumes 2>/dev/null; true

update-expected-csv: ensure-jar
	@$(DC) -f csv/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f csv/docker-compose.yml \
	  run --rm --build -e "PGLOADER_CMD=pgloader --client-min-messages notice" -e REGRESS_FLAGS=--update test-runner
	@$(DC) -f csv/docker-compose.yml down --volumes 2>/dev/null; true

# ─── Suite: copy ─────────────────────────────────────────────────────────────
# PostgreSQL COPY format files.

copy: ensure-jar
	@$(DC) -f copy/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f copy/docker-compose.yml run --rm --build test-runner
	@$(DC) -f copy/docker-compose.yml down --volumes 2>/dev/null; true

copy-v3: ensure-jar
	@$(DC) -f copy/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f copy/docker-compose.yml run --rm --build \
	  -e "PGLOADER_CMD=pgloader --client-min-messages notice" test-runner
	@$(DC) -f copy/docker-compose.yml down --volumes 2>/dev/null; true

update-expected-copy: ensure-jar
	@$(DC) -f copy/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f copy/docker-compose.yml \
	  run --rm --build -e "PGLOADER_CMD=pgloader --client-min-messages notice" -e REGRESS_FLAGS=--update test-runner
	@$(DC) -f copy/docker-compose.yml down --volumes 2>/dev/null; true

# ─── Suite: fixed ─────────────────────────────────────────────────────────────
# Fixed-width format files.

fixed: ensure-jar
	@$(DC) -f fixed/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f fixed/docker-compose.yml run --rm --build test-runner
	@$(DC) -f fixed/docker-compose.yml down --volumes 2>/dev/null; true

fixed-v3: ensure-jar
	@$(DC) -f fixed/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f fixed/docker-compose.yml run --rm --build \
	  -e "PGLOADER_CMD=pgloader --client-min-messages notice" test-runner make -k -f /work/Makefile all-v3
	@$(DC) -f fixed/docker-compose.yml down --volumes 2>/dev/null; true

update-expected-fixed: ensure-jar
	@$(DC) -f fixed/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f fixed/docker-compose.yml \
	  run --rm --build -e "PGLOADER_CMD=pgloader --client-min-messages notice" -e REGRESS_FLAGS=--update test-runner
	@$(DC) -f fixed/docker-compose.yml down --volumes 2>/dev/null; true

# ─── Suite: dbf ───────────────────────────────────────────────────────────────
# DBF (dBASE) files.

dbf: ensure-jar
	@$(DC) -f dbf/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f dbf/docker-compose.yml run --rm --build test-runner
	@$(DC) -f dbf/docker-compose.yml down --volumes 2>/dev/null; true

dbf-v3: ensure-jar
	@$(DC) -f dbf/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f dbf/docker-compose.yml run --rm --build \
	  -e "PGLOADER_CMD=pgloader --client-min-messages notice" test-runner make -k -f /work/Makefile all-v3
	@$(DC) -f dbf/docker-compose.yml down --volumes 2>/dev/null; true

update-expected-dbf: ensure-jar
	@$(DC) -f dbf/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f dbf/docker-compose.yml \
	  run --rm --build -e REGRESS_FLAGS=--update test-runner
	@$(DC) -f dbf/docker-compose.yml down --volumes 2>/dev/null; true

# ─── Suite: sqlite ────────────────────────────────────────────────────────────
# SQLite files.

sqlite: ensure-jar
	@$(DC) -f sqlite/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f sqlite/docker-compose.yml run --rm --build test-runner
	@$(DC) -f sqlite/docker-compose.yml down --volumes 2>/dev/null; true

sqlite-v3: ensure-jar
	@$(DC) -f sqlite/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f sqlite/docker-compose.yml run --rm --build \
	  -e "PGLOADER_CMD=pgloader --client-min-messages notice" test-runner
	@$(DC) -f sqlite/docker-compose.yml down --volumes 2>/dev/null; true

update-expected-sqlite: ensure-jar
	@$(DC) -f sqlite/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f sqlite/docker-compose.yml \
	  run --rm --build -e "PGLOADER_CMD=pgloader --client-min-messages notice" -e REGRESS_FLAGS=--update test-runner
	@$(DC) -f sqlite/docker-compose.yml down --volumes 2>/dev/null; true

# ─── Suite: mysql ─────────────────────────────────────────────────────────────
# MySQL → PostgreSQL migration (basic, cast, filters, sakila, f1db).
# Requires f1db.sql dataset.

mysql: ensure-jar ensure-f1db
	@$(DC) -f mysql/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f mysql/docker-compose.yml run --rm --build test-runner
	@$(DC) -f mysql/docker-compose.yml down --volumes 2>/dev/null; true

mysql-v3: ensure-jar ensure-f1db
	@$(DC) -f mysql/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f mysql/docker-compose.yml run --rm --build \
	  -e "PGLOADER_CMD=pgloader --client-min-messages notice" test-runner make -C /suite all-v3
	@$(DC) -f mysql/docker-compose.yml down --volumes 2>/dev/null; true

# ─── Suite: mysql57 ───────────────────────────────────────────────────────────
# MySQL 5.7 — the version CL pgloader was developed against.
# Canonical runnable spec: full CL test files without workarounds.
# amd64 only (runs under Rosetta on Apple Silicon).

mysql57: ensure-jar ensure-f1db
	@$(DC) -f mysql57/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f mysql57/docker-compose.yml run --rm --build test-runner
	@$(DC) -f mysql57/docker-compose.yml down --volumes 2>/dev/null; true

mysql57-v3: ensure-jar ensure-f1db
	@$(DC) -f mysql57/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f mysql57/docker-compose.yml run --rm --build \
	  -e "PGLOADER_CMD=pgloader --client-min-messages notice" test-runner
	@$(DC) -f mysql57/docker-compose.yml down --volumes 2>/dev/null; true

# ─── Suite: mariadb ───────────────────────────────────────────────────────────
# MariaDB → PostgreSQL migration (basic, sakila, f1db).
# Requires f1db.sql dataset.

mariadb: ensure-jar ensure-f1db
	@$(DC) -f mariadb/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f mariadb/docker-compose.yml run --rm --build test-runner
	@$(DC) -f mariadb/docker-compose.yml down --volumes 2>/dev/null; true

mariadb-v3: ensure-jar ensure-f1db
	@$(DC) -f mariadb/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f mariadb/docker-compose.yml run --rm --build \
	  -e "PGLOADER_CMD=pgloader --client-min-messages notice" test-runner make -C /suite all-v3
	@$(DC) -f mariadb/docker-compose.yml down --volumes 2>/dev/null; true

# ─── Suite: pgsql ─────────────────────────────────────────────────────────────
# PostgreSQL → PostgreSQL migration.

pgsql: ensure-jar
	@$(DC) -f pgsql/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f pgsql/docker-compose.yml run --rm --build test-runner
	@$(DC) -f pgsql/docker-compose.yml down --volumes 2>/dev/null; true

pgsql-v3: ensure-jar
	@$(DC) -f pgsql/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f pgsql/docker-compose.yml run --rm --build \
	  -e "PGLOADER_CMD=pgloader --client-min-messages notice" test-runner
	@$(DC) -f pgsql/docker-compose.yml down --volumes 2>/dev/null; true

# ─── Suite: mssql ─────────────────────────────────────────────────────────────
# SQL Server → PostgreSQL migration.

mssql: ensure-jar
	@$(DC) -f mssql/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f mssql/docker-compose.yml run --rm --build test-runner
	@$(DC) -f mssql/docker-compose.yml down --volumes 2>/dev/null; true

mssql-v3: ensure-jar
	@$(DC) -f mssql/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f mssql/docker-compose.yml run --rm --build \
	  -e "PGLOADER_CMD=pgloader --client-min-messages notice" test-runner
	@$(DC) -f mssql/docker-compose.yml down --volumes 2>/dev/null; true

# ─── Suite: citus ─────────────────────────────────────────────────────────────
# MySQL → Citus (distributed PostgreSQL) migration.

citus: ensure-jar
	@$(DC) -f citus/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f citus/docker-compose.yml run --rm --build test-runner
	@$(DC) -f citus/docker-compose.yml down --volumes 2>/dev/null; true

citus-v3: ensure-jar
	@$(DC) -f citus/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f citus/docker-compose.yml run --rm --build \
	  -e "PGLOADER_CMD=pgloader --client-min-messages notice" test-runner
	@$(DC) -f citus/docker-compose.yml down --volumes 2>/dev/null; true

# ─── Baseline registration ────────────────────────────────────────────────────
# Re-run a suite using pgloader v3 to load, then write psql query output to
# expected/ — these become the golden baselines that v4 must match.
# Run after intentional behaviour changes, then commit the results.

update-expected-mysql: ensure-jar ensure-f1db
	@$(DC) -f mysql/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f mysql/docker-compose.yml \
	  run --rm --build -e "PGLOADER_CMD=pgloader --client-min-messages notice" -e REGRESS_FLAGS=--update test-runner
	@$(DC) -f mysql/docker-compose.yml down --volumes 2>/dev/null; true

update-expected-mysql57: ensure-jar ensure-f1db
	@$(DC) -f mysql57/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f mysql57/docker-compose.yml \
	  run --rm --build -e "PGLOADER_CMD=pgloader --client-min-messages notice" -e REGRESS_FLAGS=--update test-runner
	@$(DC) -f mysql57/docker-compose.yml down --volumes 2>/dev/null; true

update-expected-mariadb: ensure-jar ensure-f1db
	@$(DC) -f mariadb/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f mariadb/docker-compose.yml \
	  run --rm --build -e "PGLOADER_CMD=pgloader --client-min-messages notice" -e REGRESS_FLAGS=--update test-runner
	@$(DC) -f mariadb/docker-compose.yml down --volumes 2>/dev/null; true

update-expected-pgsql: ensure-jar
	@$(DC) -f pgsql/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f pgsql/docker-compose.yml \
	  run --rm --build -e "PGLOADER_CMD=pgloader --client-min-messages notice" -e REGRESS_FLAGS=--update test-runner
	@$(DC) -f pgsql/docker-compose.yml down --volumes 2>/dev/null; true

update-expected-mssql: ensure-jar
	@$(DC) -f mssql/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f mssql/docker-compose.yml \
	  run --rm --build -e "PGLOADER_CMD=pgloader --client-min-messages notice" -e REGRESS_FLAGS=--update test-runner
	@$(DC) -f mssql/docker-compose.yml down --volumes 2>/dev/null; true

update-expected-citus: ensure-jar
	@$(DC) -f citus/docker-compose.yml down --volumes 2>/dev/null; true
	$(DC) $(PROFILE) -f citus/docker-compose.yml \
	  run --rm --build -e REGRESS_FLAGS=--update test-runner
	@$(DC) -f citus/docker-compose.yml down --volumes 2>/dev/null; true

# ─── Bench suite (delegates to tests/bench/Makefile) ─────────────────────────

bench employees employees-v3 lahman lahman-v3 bench-report:
	$(MAKE) -C bench $@

bench-down:
	$(MAKE) -C bench down

# ─── Teardown ─────────────────────────────────────────────────────────────────
# Stop every suite's compose stack (safe to run at any time).

down:
	@for suite in csv copy fixed dbf sqlite mysql mysql57 mariadb pgsql mssql citus; do \
	  if [ -f "$$suite/docker-compose.yml" ]; then \
	    echo "Stopping $$suite..."; \
	    $(DC) -f "$$suite/docker-compose.yml" down 2>/dev/null || true; \
	  fi; \
	done

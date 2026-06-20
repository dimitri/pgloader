.PHONY: check-env ensure-f1db build clean \
        build-test-image \
        test test-unit tests \
        down-all \
        run-csv run-mysql run-mariadb run-load-file

# ─── Environment check ───────────────────────────────────────────────────────

check-env:
	@echo "=== Checking build environment ==="
	@java -version 2>&1 | grep -oE '"([0-9]+)' | head -1 | grep -oE '[0-9]+' | \
	  while read v; do \
	    if [ "$$v" -ge 21 ]; then exit 0; else exit 1; fi; \
	  done || (echo "ERROR: JDK 21+ required." && exit 1)
	@clojure --version 2>&1 | grep -qE "Clojure CLI version 1\.(11|12)" \
	  || (echo "ERROR: Clojure CLI 1.11+ required." && exit 1)
	@docker --version >/dev/null 2>&1 \
	  || (echo "ERROR: Docker not found." && exit 1)
	@docker compose version >/dev/null 2>&1 \
	  || (echo "ERROR: Docker Compose v2 not found." && exit 1)
	@echo "=== Environment OK ==="

# ─── Dataset download ────────────────────────────────────────────────────────

ensure-f1db:
	@if [ ! -f ../test/data/f1db.sql ]; then \
	  echo "=== F1DB dataset not found — downloading (CC BY-SA 4.0, ~23 MB)... ==="; \
	  bash ../test/mysql/download-f1db.sh; \
	fi

# ─── Build ────────────────────────────────────────────────────────────────────

build: check-env
	clojure -T:build uber

clean:
	rm -rf target

# ─── Test runner Docker image ─────────────────────────────────────────────────
# Contains JRE 21 + psql client + make. Built once, used by all suite compose files.

build-test-image:
	$(MAKE) -C tests build-test-image

# ─── Unit tests (no Docker required) ─────────────────────────────────────────

test-unit:
	clojure -M:test -m cognitect.test-runner \
	  -n pgloader.cast-test \
	  -n pgloader.batch-test \
	  -n pgloader.ddl-test \
	  -n pgloader.load-file.parser-test \
	  -n pgloader.transforms-test \
	  -n pgloader.pg-service-test \
	  -n pgloader.cli-test

# ─── E2E integration tests ────────────────────────────────────────────────────
# All suite management lives in tests/Makefile.
# Pass ROOT so the sub-make can resolve paths back to the repo root.

tests: build
	$(MAKE) -C tests ROOT=..

test: test-unit tests

# ─── Teardown ─────────────────────────────────────────────────────────────────

down-all:
	$(MAKE) -C tests down-all

# ─── Ad-hoc run targets (build the uberjar first) ────────────────────────────

run-csv: build
	java -jar target/pgloader-v4-*.jar \
	  csv:///$(PWD)/test/fixtures/csv/sample.csv \
	  pgsql://pgloader:pgloader@localhost/target

run-mysql: build
	java -jar target/pgloader-v4-*.jar \
	  mysql://root:rootpassword@localhost:3306/sakila \
	  pgsql://pgloader:pgloader@localhost/target

run-mariadb: build
	java -jar target/pgloader-v4-*.jar \
	  mysql://root:rootpassword@localhost:3307/sakila \
	  pgsql://pgloader:pgloader@localhost/target

run-load-file: build
	java -jar target/pgloader-v4-*.jar \
	  test/pgloader/load_file/fixtures/valid/mysql_with_cast.load

# Container default for JAR; fall back to relative path for local runs
_jar_default := $(or $(wildcard /pgloader.jar),$(firstword $(wildcard ../../target/pgloader.jar ../../target/pgloader-*.jar)))
JAR          ?= $(_jar_default)
PGLOADER_V4  ?= java -jar $(JAR)
PGLOADER_V3  ?= pgloader --client-min-messages notice
RUNS         ?= 3
SUMMARY_DIR  ?= /tmp/pgloader-bench

# BENCH_DIR: directory containing this Makefile (script helpers live here)
BENCH_DIR    := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

LAHMAN_URL ?= https://github.com/jknecht/baseball-archive-sqlite/releases/download/2022/lahman_1871-2022.sqlite

# Portable millisecond epoch via Perl Time::HiRes (works on Linux and macOS).
ms_now = $$(perl -MTime::HiRes=time -e 'printf "%d\n", time()*1000')

# Inject os-wall-ms into a pgloader JSON summary file.
inject_ms = perl $(BENCH_DIR)inject-ms.pl

.PHONY: all bench employees employees-v3 lahman lahman-v3 report down

all: bench

bench: employees employees-v3 lahman lahman-v3 report

# ── employees ─────────────────────────────────────────────────────────────────

employees:
	@mkdir -p $(SUMMARY_DIR)
	@for n in $(shell seq 1 $(RUNS)); do \
	    echo "=== employees v4  run $$n/$(RUNS) ==="; \
	    SUMMARY=$(SUMMARY_DIR)/employees-v4-$$n.json; \
	    T0=$(ms_now); \
	    $(PGLOADER_V4) -S "$$SUMMARY" employees.load; \
	    T1=$(ms_now); \
	    $(inject_ms) "$$SUMMARY" "$$((T1-T0))"; \
	done

employees-v3:
	@mkdir -p $(SUMMARY_DIR)
	@for n in $(shell seq 1 $(RUNS)); do \
	    echo "=== employees v3  run $$n/$(RUNS) ==="; \
	    SUMMARY=$(SUMMARY_DIR)/employees-v3-$$n.json; \
	    T0=$(ms_now); \
	    $(PGLOADER_V3) -S "$$SUMMARY" employees.load; \
	    T1=$(ms_now); \
	    $(inject_ms) "$$SUMMARY" "$$((T1-T0))"; \
	done

# ── lahman ────────────────────────────────────────────────────────────────────

lahman: | lahman.sqlite
	@mkdir -p $(SUMMARY_DIR)
	@for n in $(shell seq 1 $(RUNS)); do \
	    echo "=== lahman v4  run $$n/$(RUNS) ==="; \
	    SUMMARY=$(SUMMARY_DIR)/lahman-v4-$$n.json; \
	    T0=$(ms_now); \
	    $(PGLOADER_V4) -S "$$SUMMARY" lahman.load; \
	    T1=$(ms_now); \
	    $(inject_ms) "$$SUMMARY" "$$((T1-T0))"; \
	done

lahman-v3: | lahman.sqlite
	@mkdir -p $(SUMMARY_DIR)
	@for n in $(shell seq 1 $(RUNS)); do \
	    echo "=== lahman v3  run $$n/$(RUNS) ==="; \
	    SUMMARY=$(SUMMARY_DIR)/lahman-v3-$$n.json; \
	    T0=$(ms_now); \
	    $(PGLOADER_V3) -S "$$SUMMARY" lahman.load; \
	    T1=$(ms_now); \
	    $(inject_ms) "$$SUMMARY" "$$((T1-T0))"; \
	done

# ── data fetch ────────────────────────────────────────────────────────────────

lahman.sqlite:
	curl -fsSL -o $@ "$(LAHMAN_URL)"

# ── report ────────────────────────────────────────────────────────────────────

report:
	@python3 $(BENCH_DIR)report.py $(SUMMARY_DIR)

# ── teardown ──────────────────────────────────────────────────────────────────

down:
	docker compose -f docker-compose.yml down --volumes

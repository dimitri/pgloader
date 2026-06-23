JAR          ?= /pgloader.jar
PGLOADER_V4  ?= java -jar $(JAR)
PGLOADER_V3  ?= pgloader --client-min-messages notice
RUNS         ?= 3
SUMMARY_DIR  ?= /tmp/pgloader-bench

LAHMAN_URL ?= https://github.com/jknecht/baseball-archive-sqlite/releases/download/2022/lahman_1871-2022.sqlite

.PHONY: all bench employees employees-v3 lahman lahman-v3 report down

all: bench

bench: employees employees-v3 lahman lahman-v3 report

# ── employees ─────────────────────────────────────────────────────────────────

employees:
	@mkdir -p $(SUMMARY_DIR)
	@for n in $(shell seq 1 $(RUNS)); do \
	    echo "=== employees v4  run $$n/$(RUNS) ==="; \
	    SUMMARY=$(SUMMARY_DIR)/employees-v4-$$n.json; \
	    T0=$$(date +%s%3N); \
	    $(PGLOADER_V4) -S "$$SUMMARY" employees.load; \
	    T1=$$(date +%s%3N); \
	    python3 -c "import json; \
	        d=json.load(open('$$SUMMARY')); \
	        d['os-wall-ms']=$$(($$T1-$$T0)); \
	        json.dump(d,open('$$SUMMARY','w'))"; \
	done

employees-v3:
	@mkdir -p $(SUMMARY_DIR)
	@for n in $(shell seq 1 $(RUNS)); do \
	    echo "=== employees v3  run $$n/$(RUNS) ==="; \
	    SUMMARY=$(SUMMARY_DIR)/employees-v3-$$n.json; \
	    T0=$$(date +%s%3N); \
	    $(PGLOADER_V3) -S "$$SUMMARY" employees.load; \
	    T1=$$(date +%s%3N); \
	    python3 -c "import json; \
	        d=json.load(open('$$SUMMARY')); \
	        d['os-wall-ms']=$$(($$T1-$$T0)); \
	        json.dump(d,open('$$SUMMARY','w'))"; \
	done

# ── lahman ────────────────────────────────────────────────────────────────────

lahman: | lahman.sqlite
	@mkdir -p $(SUMMARY_DIR)
	@for n in $(shell seq 1 $(RUNS)); do \
	    echo "=== lahman v4  run $$n/$(RUNS) ==="; \
	    SUMMARY=$(SUMMARY_DIR)/lahman-v4-$$n.json; \
	    T0=$$(date +%s%3N); \
	    $(PGLOADER_V4) -S "$$SUMMARY" lahman.load; \
	    T1=$$(date +%s%3N); \
	    python3 -c "import json; \
	        d=json.load(open('$$SUMMARY')); \
	        d['os-wall-ms']=$$(($$T1-$$T0)); \
	        json.dump(d,open('$$SUMMARY','w'))"; \
	done

lahman-v3: | lahman.sqlite
	@mkdir -p $(SUMMARY_DIR)
	@for n in $(shell seq 1 $(RUNS)); do \
	    echo "=== lahman v3  run $$n/$(RUNS) ==="; \
	    SUMMARY=$(SUMMARY_DIR)/lahman-v3-$$n.json; \
	    T0=$$(date +%s%3N); \
	    $(PGLOADER_V3) -S "$$SUMMARY" lahman.load; \
	    T1=$$(date +%s%3N); \
	    python3 -c "import json; \
	        d=json.load(open('$$SUMMARY')); \
	        d['os-wall-ms']=$$(($$T1-$$T0)); \
	        json.dump(d,open('$$SUMMARY','w'))"; \
	done

# ── data fetch ────────────────────────────────────────────────────────────────

lahman.sqlite:
	curl -fsSL -o $@ "$(LAHMAN_URL)"

# ── report ────────────────────────────────────────────────────────────────────

report:
	@python3 /suite/report.py $(SUMMARY_DIR)

# ── teardown ──────────────────────────────────────────────────────────────────

down:
	docker compose -f docker-compose.yml down --volumes

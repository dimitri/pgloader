JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

CSV_TESTS = allcols basic batch-rows before-after-do blanks-keep blanks-trim \
            column-order column-subset constant csv-escape-mode datestyle-dmy \
            disable-triggers districts drop-indexes encoding escape-backslash \
            filename-pattern guess-csv header-auto lines-terminated missing-col \
            non-printable not-enclosed null-if nulls overflow parse-date pipe \
            projection reformat semicolon set-params skip-header-2 tab \
            target-columns temp track trailing

# constant uses v4-only column injection: TARGET TABLE t (b,c,d) where c has no
# source column — v3 evaluates the unbound symbol c and errors.
# target-columns uses HAVING FIELDS + TARGET COLUMNS renaming, which v3 supports.
# stdin is v4-only (piped input) and runs in a separate step, not the batch.
# csv-escape-mode: v3 parses "csv escape mode following" differently; exclude from v3.
CSV_TESTS_V3 = $(filter-out constant csv-escape-mode,$(CSV_TESTS))

# Auto-detect v3 run: PGLOADER_CMD starts with "pgloader" (the CL binary),
# not "java" (the v4 JAR). This covers both the %-v3 pattern and Docker runs
# where the container receives PGLOADER_CMD via environment variable.
ifeq ($(firstword $(PGLOADER_CMD)),pgloader)
  _CSV_TESTS := $(CSV_TESTS_V3)
else
  _CSV_TESTS := $(CSV_TESTS)
endif

.PHONY: all csv stdin-test

all: csv stdin-test

# ── CSV: pass all load files as args to one pgloader invocation, print summary ──
csv:
	$(PGLOADER_CMD) $(foreach t,$(_CSV_TESTS),/work/$(t)/$(t).load)
	@pass=0; fail=0; names_p=""; names_f=""; \
	for t in $(_CSV_TESTS); do \
	  out=$$(java -jar $(JAR) regress $(REGRESS_FLAGS) /work/$$t 2>&1); \
	  if echo "$$out" | grep -q "^  FAIL"; then \
	    fail=$$((fail+1)); names_f="$$names_f $$t"; \
	    echo "$$out"; \
	  else \
	    pass=$$((pass+1)); names_p="$$names_p $$t"; \
	  fi; \
	done; \
	printf "\n%-30s %s\n" "csv test" "result"; \
	printf "%-30s %s\n" "------------------------------" "------"; \
	for t in $$names_p; do printf "%-30s pass\n" $$t; done; \
	for t in $$names_f; do printf "%-30s FAIL\n" $$t; done; \
	printf "\n%d passed, %d failed\n" $$pass $$fail; \
	[ $$fail -eq 0 ]

# ── STDIN (v4 only — piped input; skipped for v3) ─────────────────────────────
# The stdin test pipes a CSV file into pgloader's stdin.
# v3 does not support FROM stdin so this target is a no-op for v3 runs.
stdin-test:
ifeq ($(firstword $(PGLOADER_CMD)),pgloader)
	@echo "stdin-test: skipped (v3 does not support FROM stdin)"
else
	cat /work/stdin/stdin.csv | $(PGLOADER_CMD) /work/stdin/stdin.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) /work/stdin
endif

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

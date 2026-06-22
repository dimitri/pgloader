JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

# 04-indexes: filtered index WHERE clause — v4 only.
# v3 does not translate SQL Server partial index filters to PostgreSQL WHERE
# clauses, so that test is excluded from v3 runs.
ifeq ($(firstword $(PGLOADER_CMD)),pgloader)
  _REGRESS_EXTRA := --exclude 04-indexes
else
  _REGRESS_EXTRA :=
endif

.PHONY: all run

all: run

run:
	$(PGLOADER_CMD) mssql.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) $(_REGRESS_EXTRA) .

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

# 04-indexes: filtered index WHERE clause — v4 only.
# v3 does not translate SQL Server partial index filters to PostgreSQL WHERE
# clauses, so that test is excluded from v3 runs.
# 13-tsql-defaults, 14-cast-alter-schema: tsqldefaults database — v4 only.
# T-SQL default functions such as SYSUTCDATETIME():
# v3 copies them verbatim, fails to create the table and aborts the load, so
# mssql-tsql-defaults.load is not run with v3 either.
ifeq ($(firstword $(PGLOADER_CMD)),pgloader)
  _REGRESS_EXTRA := --exclude 04-indexes --exclude 13-tsql-defaults --exclude 14-cast-alter-schema
  _V4_ONLY_LOAD_CMD =
else
  _REGRESS_EXTRA :=
  _V4_ONLY_LOAD_CMD = $(PGLOADER_CMD) /suite/mssql-tsql-defaults.load
endif

.PHONY: all run update-expected

all: run

run:
	$(PGLOADER_CMD) /suite/mssql.load
	$(PGLOADER_CMD) /suite/mssql-filter-views.load
	$(_V4_ONLY_LOAD_CMD)
	java -jar $(JAR) regress $(REGRESS_FLAGS) $(_REGRESS_EXTRA) /suite

update-expected:
	$(PGLOADER_CMD) /suite/mssql.load
	$(PGLOADER_CMD) /suite/mssql-filter-views.load
	$(_V4_ONLY_LOAD_CMD)
	java -jar $(JAR) regress --update /suite

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

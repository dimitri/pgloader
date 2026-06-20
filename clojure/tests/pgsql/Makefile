JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

.PHONY: all populate run update-expected

all: run

# Populate the source PostgreSQL instance from various source types.
# Always use the v4 JAR for this step — it is a prerequisite, not what
# the pgsql-to-pgsql path is testing.
populate:
	java -jar $(JAR) /suite/source/load-chinook.load
	java -jar $(JAR) /suite/source/load-sqlite.load
	java -jar $(JAR) /suite/source/load-dbf.load
	java -jar $(JAR) /suite/source/load-track.load
	java -jar $(JAR) /suite/source/load-places.load

run: populate
	$(PGLOADER_CMD) /suite/pgsql.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) /suite

update-expected: populate
	$(PGLOADER_CMD) /suite/pgsql.load
	java -jar $(JAR) regress --update /suite

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

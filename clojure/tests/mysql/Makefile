JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

.PHONY: all my sakila f1db source v4only

# v4only is included in the default all target so it runs alongside the shared
# suites.  It always uses the v4 JAR directly (not PGLOADER_CMD) so it runs
# with v4 even when PGLOADER_CMD is overridden to the pgloader v3 binary.
# This lets v3 CI runs still validate v4-specific behaviours as a bonus check.
all: my sakila f1db source v4only

my:
	$(PGLOADER_CMD) my/my.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) my

sakila:
	$(PGLOADER_CMD) sakila/sakila.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) sakila

f1db:
	$(PGLOADER_CMD) f1db/f1db.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) f1db

source:
	$(PGLOADER_CMD) source/source.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) source

# v4-only tests: always loaded with the v4 JAR regardless of PGLOADER_CMD.
# These cover features not present in pgloader v3:
#   #1629  datetime(N) precision preserved  →  timestamptz(N)
#   #1403  CURRENT_TIMESTAMP(N) default stripped to keyword
#   #1280  bit literal b'0' default passes through
#   #1645  CHECK constraints migrated via ALTER TABLE ADD CONSTRAINT
#   #1560/#1196  ON UPDATE CURRENT_TIMESTAMP → BEFORE UPDATE trigger
#   #1066  hex-to-bytea cast function
#   #1522  WITHOUT TYPE cast syntax
v4only:
	java -jar $(JAR) v4only/v4only.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) v4only

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

update-expected-%:
	java -jar $(JAR) regress --update $*

JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

.PHONY: all my sakila f1db source mysql-unit-full

all: my sakila f1db source mysql-unit-full

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

# mysql-unit-full: full v4 regression suite, always run with the v4 JAR.
# Never uses $(PGLOADER_CMD) so it runs with v4 even in mixed environments.
mysql-unit-full:
	java -jar $(JAR) mysql-unit-full/mysql-unit-full.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) mysql-unit-full

# mysql-unit-full-v3: same suite against the v3 binary, compared against
# .v3 variant expected files that capture deliberate v3 vs v4 differences
# (serial vs identity, bit vs bit varying, trigger naming).
mysql-unit-full-v3:
	pgloader --client-min-messages notice mysql-unit-full/mysql-unit-full.load
	java -jar $(JAR) regress --variant v3 mysql-unit-full

all-v3: my-v3 sakila-v3 f1db-v3 source-v3 mysql-unit-full-v3
	@:

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

update-expected-%:
	java -jar $(JAR) regress --update $*

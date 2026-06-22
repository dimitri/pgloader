JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

.PHONY: all mytest sakila f1db source mytest-v3

all: mytest sakila f1db

mytest:
	$(PGLOADER_CMD) mytest/mytest.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) mytest

sakila:
	$(PGLOADER_CMD) sakila/sakila.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) sakila

f1db:
	$(PGLOADER_CMD) f1db/f1db.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) f1db

# mytest-v3: same suite against the v3 binary, compared against
# .v3 variant expected files that capture deliberate v3 vs v4 differences
# (serial vs identity, bit vs bit varying, trigger naming).
mytest-v3:
	pgloader --client-min-messages notice mytest/mytest.load
	java -jar $(JAR) regress --variant v3 mytest

all-v3: mytest-v3 sakila-v3 f1db-v3
	@:

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

update-expected-%:
	java -jar $(JAR) regress --update $*

JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

.PHONY: all my sakila f1db source v4only

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

# v4-only tests always use the v4 JAR directly, never $(PGLOADER_CMD).
v4only:
	java -jar $(JAR) v4only/v4only.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) v4only

all-v3: my-v3 sakila-v3 f1db-v3 source-v3

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

update-expected-%:
	java -jar $(JAR) regress --update $*

JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

.PHONY: all run

all: run

run:
	$(PGLOADER_CMD) citus.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) .

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

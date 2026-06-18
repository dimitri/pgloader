JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

DBF_TESTS    = dbf-31 dbf-8b reg2013 dbf-memo dbf-zip
# dbf-31: v3 exposes a _NullFlags field that javadbf doesn't; schema differs
# dbf-memo: v3 misidentifies a NUMERIC(19,5) field as integer → rejects all rows
DBF_V3_TESTS = dbf-8b reg2013 dbf-zip

.PHONY: all all-v3 $(DBF_TESTS)

all: $(DBF_TESTS)

all-v3: $(DBF_V3_TESTS)

$(DBF_TESTS):
	$(PGLOADER_CMD) /work/$@/$@.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) /work/$@

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

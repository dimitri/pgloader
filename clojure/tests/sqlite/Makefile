JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

SQLITE_TESTS = chinook test-pk sqlite chinook-noseq base64

.PHONY: all $(SQLITE_TESTS) spaced-path

all: $(SQLITE_TESTS) spaced-path

$(SQLITE_TESTS):
	$(PGLOADER_CMD) /work/$@/$@.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) /work/$@

# spaced-path: the load file uses {{SQLITE_DB}} which expands to a path
# containing a space, exercising the single-quoted source-uri fix for #1365.
spaced-path:
	SQLITE_DB="work/spaced-path/my db.sqlite" $(PGLOADER_CMD) /work/spaced-path/spaced-path.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) /work/spaced-path

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

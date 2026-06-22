JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

SQLITE_TESTS    = chinook test-pk sqlite chinook-noseq base64
SQLITE_V4_TESTS = spaced-path

.PHONY: all all-v3 $(SQLITE_TESTS) $(SQLITE_V4_TESTS)

all: $(SQLITE_TESTS) $(SQLITE_V4_TESTS)

# all-v3: excludes v4-only tests (single-quoted URI, {{VAR}} expansion)
all-v3: $(SQLITE_TESTS)

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

JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

SQLITE_TESTS = chinook test-pk sqlite chinook-noseq base64 spaced-path matviews matviews-def sqlite-cli-with

.PHONY: all $(SQLITE_TESTS)

all: $(SQLITE_TESTS)

$(SQLITE_TESTS):
	$(PGLOADER_CMD) /work/$@/$@.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) /work/$@

# spaced-path: the load file uses {{SQLITE_DB}} which expands to a path
# containing a space, exercising the single-quoted source-uri fix for #1365.
spaced-path:
	SQLITE_DB="work/spaced-path/my db.sqlite" $(PGLOADER_CMD) /work/spaced-path/spaced-path.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) /work/spaced-path

# matviews: MATERIALIZE ALL VIEWS for SQLite (#699).
# Runs the "all views" load first, then validates the named-view variant.
matviews:
	$(PGLOADER_CMD) /work/matviews/matviews.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) /work/matviews

# sqlite-cli-with: exercises --with "quote identifiers" via CLI URI args (no .load file).
# Covers Fix F (#1187): v3 ecase crash on nil type in parse-cli-options.
# v4: verifies the synthetic-load-file path routes WITH options through the grammar.
sqlite-cli-with:
	$(PGLOADER_CMD) \
	    --with "quote identifiers" \
	    --with "include drop" \
	    --with "create tables" \
	    sqlite:///work/sqlite-cli-with/cli_with.db \
	    postgresql://pgloader:pgloader@postgres:5432/target
	java -jar $(JAR) regress $(REGRESS_FLAGS) /work/sqlite-cli-with

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

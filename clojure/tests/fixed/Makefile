JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

FIXED_TESTS    = census-places fields-with-periods fixed fixed-guess
# 'fixed' uses Clojure fn syntax in USING — not supported by v3
FIXED_V3_TESTS = census-places fields-with-periods fixed-guess

.PHONY: all all-v3 $(FIXED_TESTS)

all: $(FIXED_TESTS)

all-v3: $(FIXED_V3_TESTS)

$(FIXED_TESTS):
	$(PGLOADER_CMD) /work/$@/$@.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) /work/$@

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

TESTS = $(wildcard *.load)
OUT   = $(TESTS:.load=.out)

all: $(OUT)

%.out: %.load
	$(PGLOADER) --verbose $<
	@echo

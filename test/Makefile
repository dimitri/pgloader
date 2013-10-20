TESTS = $(wildcard *.load)
OUT   = $(TESTS:.load=.out)

all: $(OUT)
	echo $(OUT)

%.out: %.load
	$(PGLOADER) --verbose $<
	@echo

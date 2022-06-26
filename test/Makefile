TMPDIR ?= /tmp
TESTS = $(wildcard *.load)
OUT   = $(TESTS:.load=.out)

REMOTE = archive.load bossa-all.load bossa.load census-places.load dbf-zip.load
LOCAL  = $(filter-out $(REMOTE:.load=.out),$(OUT))
REGRESS= allcols.load 		    \
         csv-before-after.load 	    \
         csv-districts.load 	    \
         csv-parse-date.load 	    \
         csv-error.load 	    \
         csv-escape-mode.load 	    \
         csv-filename-pattern.load  \
         csv-guess.load             \
         csv-header.load            \
         csv-json.load              \
         csv-keep-extra-blanks.load \
         csv-missing-col.load       \
         csv-non-printable.load     \
         csv-nulls.load             \
         csv-temp.load              \
         csv-trim-extra-blanks.load \
         csv-using-sexp.load        \
         csv.load 		    \
	 copy.load                  \
	 copy-hex.load              \
         dbf.load 		    \
         errors.load 		    \
         fixed.load 		    \
         fields-with-periods.load   \
         ixf.load 		    \
         overflow.load 		    \
         partial.load 		    \
         serial.load 		    \
         udc.load 		    \
         xzero.load

# Those are not included in the tests because CCL doesn't have the cp866
# encoding, and then PostgreSQL 9.6 lacks "create schema if not exists".
#
# dbf-memo.load
# dbf-31.load
# dbf-8b.load

PGLOADER ?= ../build/bin/pgloader
PGSUPERUSER ?= postgres
export DBPATH = sqlite/sqlite.db # needed by sqlite-env.load
export SOURCEFILE = data/2013_Gaz_113CDs_national.txt # needed by csv-districts-env.out

EXTRA_OPTS =
ifneq (,$(findstring ccl,$(CL)))
EXTRA_OPTS = --batch --heap-reserve 150g
endif

regress: clean-out $(addprefix regress/out/, $(REGRESS:.load=.out)) ;

clean clean-out:
	rm -f regress/out/*

local: prepare $(LOCAL)

remote: prepare $(REMOTE:.load=.out)

all: prepare $(OUT)

prepare: bossa.sql
	-dropdb -U $(PGSUPERUSER) pgloader
	-dropdb -U $(PGSUPERUSER) stocks
	-dropdb -U $(PGSUPERUSER) ip4r
	-createuser -U $(PGSUPERUSER) --superuser `whoami`
	createdb -U $(PGSUPERUSER) -O `whoami` pgloader
	createdb -U $(PGSUPERUSER) -O `whoami` stocks
	createdb -U $(PGSUPERUSER) -O `whoami` ip4r
	psql -d pgloader -c 'create schema expected'
	psql -U $(PGSUPERUSER) -d pgloader -c 'create extension ip4r'
	psql -U $(PGSUPERUSER) -d ip4r -c 'create extension ip4r'
	psql -d stocks -f bossa.sql

errors.out: errors.load
	-$(PGLOADER) $<
	@echo

nofile.out: nofile.load
	-$(PGLOADER) $<
	@echo

csv-hstore.out: csv-hstore.load
	@echo skipping $@

# sakila needs preparing a MySQL database too
$(TMPDIR)/sakila-db/sakila-schema.sql: data/sakila-db.zip
	rm -rf $(TMPDIR)/sakila-db
	unzip $< -d $(TMPDIR)

sakila: $(TMPDIR)/sakila-db/sakila-schema.sql
	-dropdb -U $(PGSUPERUSER) sakila
	-createdb -U $(PGSUPERUSER) -O `whoami` sakila
	-echo "DROP DATABASE sakila" | mysql -u root
	echo "SOURCE $(TMPDIR)/sakila-db/sakila-schema.sql" | mysql -u root
	echo "SOURCE $(TMPDIR)/sakila-db/sakila-data.sql"   | mysql -u root

sakila.out: sakila sakila.load
	-$(PGLOADER) sakila.load
	@echo

csv-districts-stdin.out: csv-districts-stdin.load
	cat  data/2013_Gaz_113CDs_national.txt | $(PGLOADER) $^

ifneq (,$(findstring ccl,$(CL)))
regress/out/dbf.out: dbf.load
	@echo "Skipping $@, CCL doesn't have CP850 encoding"
	touch $@
else
	$(PGLOADER) $(EXTRA_OPTS) --regress $<
	touch $@
endif

# General case where we do NOT expect any error
%.out: %.load
	$(PGLOADER) $<
	@echo

# Regression tests
regress/out/%.out: %.load
	$(PGLOADER) $(EXTRA_OPTS) --regress $<
	touch $@

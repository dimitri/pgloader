TESTS = $(wildcard *.load)
OUT   = $(TESTS:.load=.out)

REMOTE = archive.load bossa-all.load bossa.load census-places.load dbf-zip.load
LOCAL  = $(filter-out $(REMOTE:.load=.out),$(OUT))

PGLOADER ?= ../build/pgloader.exe

local: prepare $(LOCAL)

remote: prepare $(REMOTE:.load=.out)

all: prepare $(OUT)

sakila: ;
# 	unzip data/sakila-db.zip
# 	echo "SOURCE data/sakila-db/sakila-schema.sql" | mysql -u root
# 	echo "SOURCE data/sakila-db/sakila-data.sql"   | mysql -u root

prepare: bossa.sql sakila
	-dropdb -U postgres pgloader
	-dropdb -U postgres stocks
	-dropdb -U postgres ip4r
	-createdb -U postgres -O `whoami` pgloader
	-createdb -U postgres -O `whoami` stocks
	-createdb -U postgres -O `whoami` ip4r
	-psql -U postgres -d pgloader -c 'create extension ip4r'
	-psql -U postgres -d ip4r -c 'create extension ip4r'
	-psql -d stocks -f bossa.sql

# Some special cases where we expect errors and want to continue testing
sakila.out: sakila.load
	@echo "no MySQL installation here."

errors.out: errors.load
	-$(PGLOADER) $<
	@echo

nofile.out: nofile.load
	-$(PGLOADER) $<
	@echo

# General case where we do NOT expect any error
%.out: %.load
	$(PGLOADER) $<
	@echo

TMPDIR ?= /tmp
TESTS = $(wildcard *.load)
OUT   = $(TESTS:.load=.out)

REMOTE = archive.load bossa-all.load bossa.load census-places.load dbf-zip.load
LOCAL  = $(filter-out $(REMOTE:.load=.out),$(OUT))

PGLOADER ?= ../build/pgloader.exe

local: prepare $(LOCAL)

remote: prepare $(REMOTE:.load=.out)

all: prepare $(OUT)

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

errors.out: errors.load
	-$(PGLOADER) $<
	@echo

nofile.out: nofile.load
	-$(PGLOADER) $<
	@echo

# sakila needs preparing a MySQL database too
$(TMPDIR)/sakila-db/sakila-schema.sql: data/sakila-db.zip
	rm -rf $(TMPDIR)/sakila-db
	unzip $< -d $(TMPDIR)

sakila: $(TMPDIR)/sakila-db/sakila-schema.sql
	-dropdb -U postgres sakila
	-createdb -U postgres -O `whoami` sakila
	-echo "DROP DATABASE sakila" | mysql -u root
	echo "SOURCE $(TMPDIR)/sakila-db/sakila-schema.sql" | mysql -u root
	echo "SOURCE $(TMPDIR)/sakila-db/sakila-data.sql"   | mysql -u root

sakila.out: sakila sakila.load
	-$(PGLOADER) sakila.load
	@echo

# General case where we do NOT expect any error
%.out: %.load
	$(PGLOADER) $<
	@echo

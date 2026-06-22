JAR          ?= /pgloader.jar
PGLOADER_CMD ?= java -jar $(JAR)
REGRESS_FLAGS ?=

.PHONY: all my sakila f1db mysql-unit-full

all: my sakila f1db mysql-unit-full

my:
	$(PGLOADER_CMD) my/my.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) my

sakila:
	$(PGLOADER_CMD) sakila/sakila.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) sakila

f1db:
	$(PGLOADER_CMD) f1db/f1db.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) f1db

# mysql-unit-full: full v4 regression suite, always run with the v4 JAR.
# Runs against MariaDB (MYSQL_HOST=mariadb set in docker-compose.yml).
# Uses --variant mariadb so tests whose output differs from MySQL 8 (e.g.
# CHECK constraints surfaced differently) match their own expected files,
# while tests with identical output fall back to the shared baseline.
mysql-unit-full:
	java -jar $(JAR) mysql-unit-full/mysql-unit-full.load
	java -jar $(JAR) regress $(REGRESS_FLAGS) --variant mariadb mysql-unit-full

# mysql-unit-full-v3: same suite against the v3 binary on MariaDB, compared
# against .v3.mariadb variant expected files.
mysql-unit-full-v3:
	pgloader --client-min-messages notice mysql-unit-full/mysql-unit-full.load
	java -jar $(JAR) regress --variant v3.mariadb mysql-unit-full

all-v3: my-v3 sakila-v3 f1db-v3 mysql-unit-full-v3
	@:

%-v3:
	$(MAKE) $* PGLOADER_CMD="pgloader --client-min-messages notice"

%-v3-debug:
	$(MAKE) $* PGLOADER_CMD="pgloader --debug --client-min-messages debug"

update-expected-%:
	java -jar $(JAR) regress --update $*

update-expected-mariadb-%:
	java -jar $(JAR) regress --update --variant mariadb $*

# MariaDB with pre-loaded datasets from local repo files
FROM mariadb:11

# Install unzip
RUN apt-get update && apt-get install -y unzip && rm -rf /var/lib/apt/lists/*

# === Sakila dataset (from test/data/sakila-db.zip) ===
COPY test/data/sakila-db.zip /tmp/sakila-db.zip
RUN unzip /tmp/sakila-db.zip -d /tmp/ \
  && mv /tmp/sakila-db/sakila-schema.sql /docker-entrypoint-initdb.d/sakila-schema.sql \
  && mv /tmp/sakila-db/sakila-data.sql /docker-entrypoint-initdb.d/sakila-data.sql \
  && rm -rf /tmp/sakila-db /tmp/sakila-db.zip

# === F1DB dataset (from test/data/f1db.sql, Ergast 2024) ===
# Download with: test/mysql/download-f1db.sh
COPY test/data/f1db.sql /docker-entrypoint-initdb.d/f1db.sql

# === pgloader test schemas (from test/mysql/) ===
COPY test/mysql/my.sql       /docker-entrypoint-initdb.d/my.sql
COPY test/mysql/history.sql  /docker-entrypoint-initdb.d/history.sql

# === db789 materialized views schema (from test/mysql/) ===
COPY test/mysql/db789.sql    /docker-entrypoint-initdb.d/db789.sql

# === pgloader integration test seed (source + source2 databases) ===
COPY clojure/tests/mariadb/data/seed.sql /docker-entrypoint-initdb.d/seed.sql

# === mysql-unit-full test schema (pgloader_mysql_unit_full database) ===
COPY clojure/tests/mysql-unit-full/mysql-unit-full.sql /docker-entrypoint-initdb.d/mysql-unit-full.sql

# === Init script ===
# .sql files renamed to .tmpl to prevent MariaDB auto-running them.
# Our shell script creates the correct databases first, then sources the .tmpl files.
COPY clojure/tests/mariadb/data/load-datasets.sh /docker-entrypoint-initdb.d/load-datasets.sh

RUN chmod +x /docker-entrypoint-initdb.d/load-datasets.sh \
  && mv /docker-entrypoint-initdb.d/sakila-schema.sql /docker-entrypoint-initdb.d/sakila-schema.tmpl \
  && mv /docker-entrypoint-initdb.d/sakila-data.sql /docker-entrypoint-initdb.d/sakila-data.tmpl \
  && mv /docker-entrypoint-initdb.d/f1db.sql /docker-entrypoint-initdb.d/f1db.tmpl \
  && mv /docker-entrypoint-initdb.d/my.sql        /docker-entrypoint-initdb.d/my.tmpl \
  && mv /docker-entrypoint-initdb.d/history.sql   /docker-entrypoint-initdb.d/history.tmpl \
  && mv /docker-entrypoint-initdb.d/db789.sql     /docker-entrypoint-initdb.d/db789.tmpl \
  && mv /docker-entrypoint-initdb.d/seed.sql      /docker-entrypoint-initdb.d/seed.tmpl \
  && mv /docker-entrypoint-initdb.d/mysql-unit-full.sql /docker-entrypoint-initdb.d/mysql-unit-full.tmpl

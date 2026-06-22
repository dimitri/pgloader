# MySQL with pre-loaded datasets from local repo files
FROM mysql:8.0

# Install unzip
RUN microdnf install -y unzip && microdnf clean all

# === Sakila dataset (from test/data/sakila-db.zip) ===
COPY test/data/sakila-db.zip /tmp/sakila-db.zip
RUN unzip /tmp/sakila-db.zip -d /tmp/ \
  && mv /tmp/sakila-db/sakila-schema.sql /docker-entrypoint-initdb.d/sakila-schema.sql \
  && mv /tmp/sakila-db/sakila-data.sql /docker-entrypoint-initdb.d/sakila-data.sql \
  && rm -rf /tmp/sakila-db /tmp/sakila-db.zip

# === F1DB dataset (from test/data/f1db.sql, Ergast 2024) ===
# Download with: test/mysql/download-f1db.sh
COPY test/data/f1db.sql /docker-entrypoint-initdb.d/f1db.sql

# === db789 materialized views schema (from test/mysql/) ===
COPY test/mysql/db789.sql /docker-entrypoint-initdb.d/db789.sql

# === Unified pgloader_mytest test schema ===
# Combines: legacy "my" + "history" tables, former source/source2 tables,
# and the mysql-unit-full feature tests.  See mytest/mytest.sql for details.
COPY clojure/tests/mysql/mytest/mytest.sql /docker-entrypoint-initdb.d/mytest.sql

# === Init script ===
# .sql files renamed to .tmpl to prevent MySQL auto-running them.
# Our shell script creates the correct databases first, then sources the .tmpl files.
COPY clojure/tests/mysql/data/load-datasets.sh /docker-entrypoint-initdb.d/load-datasets.sh

RUN chmod +x /docker-entrypoint-initdb.d/load-datasets.sh \
  && mv /docker-entrypoint-initdb.d/sakila-schema.sql /docker-entrypoint-initdb.d/sakila-schema.tmpl \
  && mv /docker-entrypoint-initdb.d/sakila-data.sql   /docker-entrypoint-initdb.d/sakila-data.tmpl \
  && mv /docker-entrypoint-initdb.d/f1db.sql          /docker-entrypoint-initdb.d/f1db.tmpl \
  && mv /docker-entrypoint-initdb.d/db789.sql         /docker-entrypoint-initdb.d/db789.tmpl \
  && mv /docker-entrypoint-initdb.d/mytest.sql        /docker-entrypoint-initdb.d/mytest.tmpl

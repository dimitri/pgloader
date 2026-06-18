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

# === pgloader test schemas (from test/mysql/) ===
COPY test/mysql/my.sql       /docker-entrypoint-initdb.d/my.sql
COPY test/mysql/history.sql  /docker-entrypoint-initdb.d/history.sql

# === db789 materialized views schema (from test/mysql/) ===
COPY test/mysql/db789.sql    /docker-entrypoint-initdb.d/db789.sql

# === pgloader integration test seed (source + source2 databases) ===
COPY clojure/tests/mysql/data/seed.sql /docker-entrypoint-initdb.d/seed.sql

# === Init script ===
# .sql files renamed to .tmpl to prevent MySQL auto-running them.
# Our shell script creates the correct databases first, then sources the .tmpl files.
RUN cat > /docker-entrypoint-initdb.d/load-datasets.sh << 'SCRIPT'
#!/bin/bash
set -e

echo "=== Loading Sakila dataset ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/sakila-schema.tmpl
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/sakila-data.tmpl
echo "Sakila loaded successfully"

echo "=== Loading F1DB dataset (14 tables) ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/f1db.tmpl
echo "F1DB loaded successfully"

echo "=== Loading pgloader test schemas (my, history) ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "CREATE DATABASE IF NOT EXISTS pgloader"
mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "SET GLOBAL sql_mode=''" pgloader
mysql -u root -p"$MYSQL_ROOT_PASSWORD" pgloader < /docker-entrypoint-initdb.d/my.tmpl
mysql -u root -p"$MYSQL_ROOT_PASSWORD" pgloader < /docker-entrypoint-initdb.d/history.tmpl
echo "pgloader test schemas loaded successfully"

echo "=== Loading db789 materialized views schema ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/db789.tmpl
echo "db789 loaded successfully"

echo "=== Loading pgloader integration seed (source, source2) ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/seed.tmpl
echo "Seed loaded successfully"
SCRIPT

RUN chmod +x /docker-entrypoint-initdb.d/load-datasets.sh \
  && mv /docker-entrypoint-initdb.d/sakila-schema.sql /docker-entrypoint-initdb.d/sakila-schema.tmpl \
  && mv /docker-entrypoint-initdb.d/sakila-data.sql /docker-entrypoint-initdb.d/sakila-data.tmpl \
  && mv /docker-entrypoint-initdb.d/f1db.sql /docker-entrypoint-initdb.d/f1db.tmpl \
  && mv /docker-entrypoint-initdb.d/my.sql        /docker-entrypoint-initdb.d/my.tmpl \
  && mv /docker-entrypoint-initdb.d/history.sql   /docker-entrypoint-initdb.d/history.tmpl \
  && mv /docker-entrypoint-initdb.d/db789.sql     /docker-entrypoint-initdb.d/db789.tmpl \
  && mv /docker-entrypoint-initdb.d/seed.sql      /docker-entrypoint-initdb.d/seed.tmpl

# MySQL 5.7 — the version CL pgloader was developed and tested against.
# amd64 only; runs under Rosetta on Apple Silicon.
FROM --platform=linux/amd64 mysql:5.7

# Install unzip (Oracle Linux 7 base)
RUN yum install -y unzip && yum clean all

# Disable only_full_group_by — enabled by default since 5.7.5 but the sakila
# sample schema predates it (MySQL 5.0 era). CL pgloader was tested against
# MySQL 5.6 where this mode was off by default.
RUN printf '[mysqld]\nsql_mode=STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION\n' \
    > /etc/mysql/conf.d/sql-mode.cnf

# === Sakila dataset ===
COPY test/data/sakila-db.zip /tmp/sakila-db.zip
RUN unzip /tmp/sakila-db.zip -d /tmp/ \
  && mv /tmp/sakila-db/sakila-schema.sql /docker-entrypoint-initdb.d/sakila-schema.sql \
  && mv /tmp/sakila-db/sakila-data.sql   /docker-entrypoint-initdb.d/sakila-data.sql \
  && rm -rf /tmp/sakila-db /tmp/sakila-db.zip

# === F1DB dataset ===
# Strip MySQL 8-only collation utf8mb4_0900_ai_ci — not available in MySQL 5.7
COPY test/data/f1db.sql /tmp/f1db-src.sql
RUN sed 's/utf8mb4_0900_ai_ci/utf8mb4_general_ci/g; s/ ENCRYPTION=.N.//g' \
    /tmp/f1db-src.sql > /docker-entrypoint-initdb.d/f1db.sql \
  && rm /tmp/f1db-src.sql

# === pgloader test schemas (from test/mysql/) ===
COPY test/mysql/my.sql      /docker-entrypoint-initdb.d/my.sql
COPY test/mysql/history.sql /docker-entrypoint-initdb.d/history.sql

# === db789 materialized views schema ===
COPY test/mysql/db789.sql /docker-entrypoint-initdb.d/db789.sql

# === Init script ===
RUN cat > /docker-entrypoint-initdb.d/load-datasets.sh << 'SCRIPT'
#!/bin/bash
set -e

echo "=== Loading Sakila ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/sakila-schema.tmpl
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/sakila-data.tmpl

echo "=== Loading F1DB ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/f1db.tmpl

echo "=== Loading pgloader test schemas ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "CREATE DATABASE IF NOT EXISTS pgloader"
mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "SET GLOBAL sql_mode=''" pgloader
mysql -u root -p"$MYSQL_ROOT_PASSWORD" pgloader < /docker-entrypoint-initdb.d/my.tmpl
mysql -u root -p"$MYSQL_ROOT_PASSWORD" pgloader < /docker-entrypoint-initdb.d/history.tmpl

echo "=== Loading db789 ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/db789.tmpl

echo "=== Granting pgloader access ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "
  GRANT SELECT ON pgloader.* TO 'pgloader'@'%';
  GRANT SELECT ON sakila.*   TO 'pgloader'@'%';
  GRANT SELECT ON f1db.*     TO 'pgloader'@'%';
  FLUSH PRIVILEGES;
"
SCRIPT

RUN chmod +x /docker-entrypoint-initdb.d/load-datasets.sh \
  && mv /docker-entrypoint-initdb.d/sakila-schema.sql /docker-entrypoint-initdb.d/sakila-schema.tmpl \
  && mv /docker-entrypoint-initdb.d/sakila-data.sql   /docker-entrypoint-initdb.d/sakila-data.tmpl \
  && mv /docker-entrypoint-initdb.d/f1db.sql          /docker-entrypoint-initdb.d/f1db.tmpl \
  && mv /docker-entrypoint-initdb.d/my.sql            /docker-entrypoint-initdb.d/my.tmpl \
  && mv /docker-entrypoint-initdb.d/history.sql       /docker-entrypoint-initdb.d/history.tmpl \
  && mv /docker-entrypoint-initdb.d/db789.sql         /docker-entrypoint-initdb.d/db789.tmpl

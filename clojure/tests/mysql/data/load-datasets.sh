#!/bin/bash
set -e

echo "=== Loading Sakila dataset ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/sakila-schema.tmpl
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/sakila-data.tmpl
echo "Sakila loaded successfully"

echo "=== Loading F1DB dataset (14 tables) ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/f1db.tmpl
echo "F1DB loaded successfully"

echo "=== Loading db789 materialized views schema ==="
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/db789.tmpl
echo "db789 loaded successfully"

echo "=== Loading pgloader_mytest unified test schema ==="
# Disable strict mode for the session so legacy zero-date values are accepted.
mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "SET GLOBAL sql_mode=''"
mysql -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/mytest.tmpl
echo "pgloader_mytest loaded successfully"

# NOTE — qmynd authentication (pgloader v3 / Common Lisp):
#   MySQL 8 defaults to caching_sha2_password, which qmynd does not support.
#   This container starts with --default-authentication-plugin=mysql_native_password
#   (see docker-compose.yml) so the pgloader user is already compatible.
#   If you're running against a plain MySQL 8 without that flag, run:
#
#     ALTER USER 'pgloader'@'%' IDENTIFIED WITH mysql_native_password BY 'pgloader';
#     FLUSH PRIVILEGES;

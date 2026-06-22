#!/bin/bash
set -e

echo "=== Loading Sakila dataset ==="
mariadb -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/sakila-schema.tmpl
mariadb -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/sakila-data.tmpl
echo "Sakila loaded successfully"

echo "=== Loading F1DB dataset (14 tables) ==="
mariadb -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/f1db.tmpl
echo "F1DB loaded successfully"

echo "=== Loading pgloader test schemas (my, history) ==="
mariadb -u root -p"$MYSQL_ROOT_PASSWORD" -e "CREATE DATABASE IF NOT EXISTS pgloader"
mariadb -u root -p"$MYSQL_ROOT_PASSWORD" -e "SET GLOBAL sql_mode=''" pgloader
mariadb -u root -p"$MYSQL_ROOT_PASSWORD" pgloader < /docker-entrypoint-initdb.d/my.tmpl
mariadb -u root -p"$MYSQL_ROOT_PASSWORD" pgloader < /docker-entrypoint-initdb.d/history.tmpl
echo "pgloader test schemas loaded successfully"

echo "=== Loading db789 materialized views schema ==="
mariadb -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/db789.tmpl
echo "db789 loaded successfully"

echo "=== Loading pgloader integration seed (source, source2) ==="
mariadb -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/seed.tmpl
echo "Seed loaded successfully"

echo "=== Loading mysql-unit-full test schema (pgloader_mysql_unit_full) ==="
mariadb -u root -p"$MYSQL_ROOT_PASSWORD" < /docker-entrypoint-initdb.d/mysql-unit-full.tmpl
echo "mysql-unit-full test schema loaded successfully"

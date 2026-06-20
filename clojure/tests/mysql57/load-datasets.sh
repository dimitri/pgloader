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

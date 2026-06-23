#!/bin/bash
# Called by MariaDB's docker-entrypoint.sh during first-start initialisation.
# employees.sql uses MySQL CLI "source" commands which are not valid SQL and
# are silently ignored when piped to mariadb(1).  We strip those lines and
# run the DDL portion directly, then load each data file as plain SQL.
set -e

echo "=== Loading employees schema ==="
grep -v '^source ' /employees-data/employees.sql \
    | mariadb -u root -p"${MYSQL_ROOT_PASSWORD}"

echo "=== Loading employees data files ==="
for dump in departments employees dept_emp dept_manager \
            titles salaries1 salaries2 salaries3; do
    echo "  -> load_${dump}.dump"
    mariadb -u root -p"${MYSQL_ROOT_PASSWORD}" employees \
        < /employees-data/load_${dump}.dump
done

echo "=== employees database ready ==="

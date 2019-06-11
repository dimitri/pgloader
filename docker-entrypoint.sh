#!/bin/bash
create_file () {
  if [ "$LOAD_FILE" != "" ]; then
cat <<EOF> $LOAD_FILE
load database
  from sqlite:///srv/$file
  into postgres://$PG_USER:$PG_PASS@$HOSTNAME:$PORT/$DB

WITH include drop, create tables, no truncate,
  batch rows = $BATCH_ROWS, batch concurrency = $BATCH_CONCURRENCY,
  create indexes, reset sequences, foreign keys

--SET maintenance_work_mem to '$MAIN_MEM', work_mem to '$WORK_MEM', search_path to '$SCHEMA'
  SET search_path to '$SCHEMA'

ALTER TABLE NAMES MATCHING 'account_vesting' IN SCHEMA '$SCHEMA' RENAME TO 'account_vesting_backup'
ALTER TABLE NAMES MATCHING 'accounts' IN SCHEMA '$SCHEMA' RENAME TO 'accounts_backup'
ALTER TABLE NAMES MATCHING 'db_version' IN SCHEMA '$SCHEMA' RENAME TO 'db_version_backup'
ALTER TABLE NAMES MATCHING 'history' IN SCHEMA '$SCHEMA' RENAME TO 'history_backup'
ALTER TABLE NAMES MATCHING 'name_records' IN SCHEMA '$SCHEMA' RENAME TO 'name_records_backup'
ALTER TABLE NAMES MATCHING 'namespaces' IN SCHEMA '$SCHEMA' RENAME TO 'namespaces_backup'
ALTER TABLE NAMES MATCHING 'preorders' IN SCHEMA '$SCHEMA' RENAME TO 'preorders_backup'
ALTER TABLE NAMES MATCHING 'history' IN SCHEMA '$SCHEMA' RENAME TO 'history_backup'

BEFORE LOAD DO
\$\$ CREATE SCHEMA IF NOT EXISTS $SCHEMA; \$\$;
EOF
  fi
}

inotifywait -m -e create /srv |
while read path action file; do
  if [[ $file =~ $DB_1.[0-9]* ]] || [[ $file =~ $DB_2.[0-9]* ]]; then
    echo "File matched: $file"
    BLOCK=$(echo $file | cut -f4 -d ".")
    LOAD_FILE="/srv/`date +'%Y%m%d%H'`_$BLOCK.load"
    if [ ! -f "$LOAD_FILE" ]; then
      create_file
      if [ -f "$LOAD_FILE" ]; then
        pgloader $LOAD_FILE
      fi
    fi
  fi
  if [ -f "$LOAD_FILE" ]; then
    echo "Removing loadfile"
    rm $LOAD_FILE
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'ALTER TABLE subdomain_records ALTER COLUMN block_height TYPE INT;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'DROP TABLE subdomain_records;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'ALTER TABLE subdomain_records_backup RENAME TO subdomain_records;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'CREATE TABLE subdomain_records_backup AS TABLE subdomain_records WITH NO DATA;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'DROP TABLE account_vesting;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'ALTER TABLE account_vesting_backup RENAME TO account_vesting;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'CREATE TABLE account_vesting_backup AS TABLE account_vesting WITH NO DATA;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'DROP TABLE accounts;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'ALTER TABLE accounts_backup RENAME TO accounts;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'CREATE TABLE accounts_backup AS TABLE accounts WITH NO DATA;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'DROP TABLE db_version;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'ALTER TABLE db_version_backup RENAME TO db_version;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'CREATE TABLE db_version_backup AS TABLE db_version WITH NO DATA;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'DROP TABLE history;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'ALTER TABLE history_backup RENAME TO history;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'CREATE TABLE history_backup AS TABLE history WITH NO DATA;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'DROP TABLE name_records;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'ALTER TABLE name_records_backup RENAME TO name_records;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'CREATE TABLE name_records_backup AS TABLE name_records WITH NO DATA;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'DROP TABLE namespaces;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'ALTER TABLE namespaces_backup RENAME TO namespaces;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'CREATE TABLE namespaces_backup AS TABLE namespaces WITH NO DATA;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'DROP TABLE preorders;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'ALTER TABLE preorders_backup RENAME TO preorders;'`
    CMD=`PGPASSWORD=$PG_PASS psql --host $HOSTNAME -p $PORT -U postgres $DB -c 'CREATE TABLE preorders_backup AS TABLE preorders WITH NO DATA;'`
    if [ $? -eq 0 ]; then
      echo "customization completed"
    fi
  fi
done
exit 0

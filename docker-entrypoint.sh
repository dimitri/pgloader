#!/bin/bash
SQL_FILE="/srv/move_tables.sql"

create_file () {
  if [ "$LOAD_FILE" != "" ]; then
cat <<EOF> $LOAD_FILE
load database
  from sqlite:///srv/$file
  into postgres://$PG_USER:$PG_PASS@$HOSTNAME:$PORT/$DB

WITH include drop, create tables, no truncate, disable triggers,
  batch rows = $BATCH_ROWS, batch concurrency = $BATCH_CONCURRENCY,
  create indexes, reset sequences, foreign keys

--SET maintenance_work_mem to '${MAIN_MEM}MB', work_mem to '${WORK_MEM}MB', search_path to 'temp'
  SET search_path to 'temp'

CAST type integer to int

BEFORE LOAD DO
\$\$ CREATE SCHEMA IF NOT EXISTS temp; \$\$,
\$\$ CREATE SCHEMA IF NOT EXISTS $SCHEMA; \$\$;
EOF
  fi
  if [ -f $SQL_FILE ];then
    rm /srv/move_tables.sql
  fi
cat <<EOF> $SQL_FILE
DO \$\$DECLARE row record;
BEGIN
    FOR row IN SELECT tablename FROM pg_tables WHERE schemaname = 'temp'
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS $SCHEMA.' || quote_ident(row.tablename) || ' CASCADE;';
        EXECUTE 'ALTER TABLE temp.' || quote_ident(row.tablename) || ' SET SCHEMA $SCHEMA;';
    END LOOP;
END\$\$;
EOF
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
    CMD=`PGPASSWORD=$PG_PASS psql -h $HOSTNAME -p $PORT -U $USER -d $DB -a -f $SQL_FILE`
    if [ $? -eq 0 ]; then
      echo "Customization completed. Deleting $LOAD_FILE"
      rm $LOAD_FILE
    fi
  fi
done
exit 0

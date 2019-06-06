#!/bin/bash
create_file () {
  if [ "$LOAD_FILE" != "" ]; then
cat <<EOF> $LOAD_FILE
load database
  from sqlite:///srv/$file
  into postgres://$PG_USER:$PG_PASS@$HOSTNAME/$DB

WITH include drop, create tables, no truncate,
  batch rows = $BATCH_ROWS, batch concurrency = $BATCH_CONCURRENCY,
  create indexes, reset sequences, foreign keys

--SET maintenance_work_mem to '$MAIN_MEM', work_mem to '$WORK_MEM', search_path to '$SCHEMA'
  SET search_path to '$SCHEMA'

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
    if [ $? -eq 0 ]; then
      echo "customization completed"
    fi
  fi
done
exit 0

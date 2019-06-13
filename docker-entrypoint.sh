#!/bin/bash
# rename these files to be based on subdomain or blockstack-server 
SQL_FILE="/srv/move_tables.sql"
# SQL_FILE="/srv/move_tables.sql"
LOAD_FILE="/srv/run1.load"
# SQL_FILE="/srv/move_tables.sql"

create_file () {
  # logic here for naming the env vars 
  if [ -f "$LOAD_FILE" ]; then
    rm "$LOAD_FILE"
  fi
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

  if [ -f "$SQL_FILE" ];then
    rm "$SQL_FILE"
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

run_psql () {
  CMD=`PGPASSWORD=$PG_PASS psql -h $HOSTNAME -p $PORT -U $PG_USER -d $DB -a -f $SQL_FILE`
  RET=$?
  if [ $RET -eq 0 ]; then
    echo "Customization completed. Deleting files"
    if [ -f "$SQL_FILE" ]; then
      echo "  deleting $SQL_FILE"
      rm "$SQL_FILE"
    fi
    if [ -f "$LOAD_FILE" ]; then
      echo "  deleting $LOAD_FILE"
      rm "$LOAD_FILE"
    fi
    # remove lock file to enable processing 
    # of other database named files created at the same time
    if [ -f "$LOCK" ]; then
      echo "  deleting $LOCK file"
      rm "$LOCK"
    fi
  fi
}

inotifywait -m /srv \
  --exclude '.*(\.load|~)' \
  --exclude '.*(\.sql|~)' \
  --exclude '.*(\.sh|~)' \
  # lock file name has a txt extension
  # --exclude '.*(\.txt|~)'\
  -e create \
  --format '%f %e %T' --timefmt '%H%M%S' |
while read file event tm; do
  if [[ $file == *.db.bak.[0-9]* ]]; then
    TIME_CHECK=$(date +'%H%M%S')
    # need to ensure here that if db files are written within 1 second of each other
    # we process both, and not just a single file
    #   i.e.
    #     subdomains and blockstack-server db files shoud both be written
    #     even if they are both written at the same second.
    #     this time_check will fail that scenario
    # if [ $tm -eq $TIME_CHECK ]; then
    BLOCK=$(echo $file | cut -f4 -d ".")
    # subdomain based lockfile instead of timestamp approach   
    LOCK=$(echo $file | cut -f1 -d ".")
    if [ -f "$LOCK" ]; then
      # echo "Time check passed..."
      # trying out lock files instead of timestamps 
      echo "$LOCK file exists..."
      echo "  Found file: $file"
      create_file
      if [ -f "$LOAD_FILE" ]; then
        pgloader $LOAD_FILE
        run_psql
        echo ""
        echo "Completed run on $(date)"
        echo "**********************************************"
        echo ""
      fi
    fi
    sleep 5
  fi
done
exit 0

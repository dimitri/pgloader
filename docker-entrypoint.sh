#!/bin/bash
create_file () {
  SQL_FILE="/srv/$(echo $file | cut -f1 -d ".").sql"
  LOAD_FILE="/srv/$(echo $file | cut -f1 -d ".").load"
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

# this file uses tmp schemas to drop and rename in prod for latency reduction via a local table name overwrite 
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
  --exclude '.*(\.lock|~)'\
  -e modify \
  --format '%f %e %T' --timefmt '%H%M%S' |
while read file event tm; do
  if [[ $file == *.db.bak.[0-9]* ]]; then
    # originally we approach with timestamps but used lockfiles for handling
    # edge cases instead. The variables associated with this approach are here
    # for potential future use per jwileys request! 
    # TIME_CHECK=$(date +'%H%M%S')
    # BLOCK=$(echo $file | cut -f4 -d ".")
    # subdomain based lockfile instead of timestamp approach   
    LOCK="$(echo $file | cut -f1 -d ".").lock"
    if [ ! -f "$LOCK" ]; then
      # the lock file accounts for multiple creation of 
      # a) a subdomain or blockstack-server.db being uploaded while a migration is currently ongoing
      # b) subdomain and blockstack-server being created at the same time 
      # the intended output is post load file removal (in above function) inotify will 
      # detect the other load files (named based on their db name/defined in ENV VARs in create function above 
      touch $LOCK
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

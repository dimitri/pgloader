#!/bin/bash

create_file () {
  if [ "$LOAD_FILE" != "" ]; then
cat <<EOF> $LOAD_FILE
load database
  from sqlite:///srv/$file
  into postgres://$PG_USER:$PG_PASS@$HOSTNAME/$DB

WITH include drop, create tables, no truncate,
  batch rows = 10000, batch concurrency = 10,
  create indexes, reset sequences, foreign keys

--SET maintenance_work_mem to '1024MB', work_mem to '128MB', search_path to '$SCHEMA'
  SET search_path to '$SCHEMA'

BEFORE LOAD DO
\$\$ CREATE SCHEMA IF NOT EXISTS $SCHEMA; \$\$;
EOF
  fi
}

# inotifywait -m  /srv |
inotifywait -m -e create /srv |
while read path action file; do
  if [[ $file =~ blockstack-server.db.bak.[0-9]* ]] || [[ $file =~ subdomains.db.bak.[0-9]* ]]; then
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
    CMD=`PGPASSWORD=$PG_PASS psql --host host.docker.internal -p 5433 -U postgres blockstack_core -c 'ALTER TABLE subdomain_records ALTER COLUMN block_height TYPE INT;'`
    if [ $? -eq 0 ]; then
      echo "subdomains_records ALTER block_height to INT completed"
    fi
  fi
done
exit 0

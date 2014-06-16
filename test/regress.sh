#! /bin/bash

# regress test driver
#  - run pgloader on the given .load command file
#  - parse the PostgreSQL connection string and target table
#  - output a CSV for the target table
#  - diff the CSV and error if diffs found

set -x

pgloader=$1
command=$2
targetdb=`gawk -F '[ ?]+' '/^ *INTO|into/ {print $3}' < $command`
table=`gawk -F '[ ?]+' '/^ *INTO|into/ {print $4}' < $command`

expected=regress/expected/`basename $2 .load`.out
out=regress/out/`basename $2 .load`.out

$pgloader $command
psql -c "copy $table to stdout" -d "$targetdb" > $out
diff -c $expected $out

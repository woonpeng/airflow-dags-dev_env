#!/bin/bash
set -euo pipefail

# defaults
BACKUP_FILENAME=""
if [[ -z $graphname ]]; then
  echo "<graphname> must be specified in environment variables or as a parameter" >&2
  exit 2
fi
if [[ -z $backuppath ]]; then
  echo "<backuppath> must be specified in environment variables or as a parameter" >&2
  exit 2
fi

# read-only check
if [[ $curr_read_only == true || $curr_read_only == True || $curr_read_only == 1 ]]; then
  echo "Set dbms.read_only to false to run this command" >&2
  exit 2
fi

mkdir -p $backuppath && \
service neo4j stop && \
  queryresult=$(neo4j-admin check-consistency --database=$graphname | grep "record format from store") && \
  regex='record format from store (.*)' && \
  [[ $queryresult =~ $regex ]] && \
  dbaddress=${BASH_REMATCH[1]} && \
  dbbase=$(dirname "$(dirname $dbaddress)")&& \
  pushd $backuppath && \
    if [ $# -eq 0 ] || [ -z "$1" ] ; then \
      BACKUP_FILENAME=$(date -I).dump
    else \
      RAW=$1
      BACKUP_FILENAME="${RAW//\//_}".dump
    fi && \
    rm -f "$BACKUP_FILENAME" && \
    printf "Backing up database to $BACKUP_FILENAME\n" && \
    neo4j-admin dump --database=$graphname --to="$BACKUP_FILENAME" && \
  popd
service neo4j start

source wait-for-db.sh -p $boltport

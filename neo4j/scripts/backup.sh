#!/bin/bash
set -euo pipefail

BACKUP_FILENAME=""

mkdir -p {{pipeline.neo4j.backup_path}} && \
service neo4j stop && \
  queryresult=$(neo4j-admin check-consistency --database=neo4j | grep "record format from store") && \
  regex='record format from store (.*)' && \
  [[ $queryresult =~ $regex ]] && \
  dbaddress=${BASH_REMATCH[1]} && \
  graphname=$(basename $dbaddress) && \
  pushd {{pipeline.neo4j.backup_path}} && \
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

source {{pipeline.neo4j.scripts_path}}/wait-for-db.sh

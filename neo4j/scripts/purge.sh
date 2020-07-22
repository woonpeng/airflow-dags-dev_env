#!/bin/bash
set -euo pipefail

service neo4j stop && \
  queryresult=$(neo4j-admin check-consistency --database=neo4j | grep "record format from store") && \
  regex='record format from store (.*)' && \
  [[ $queryresult =~ $regex ]] && \
  dbaddress=${BASH_REMATCH[1]} && \
  # for our purposes graphname=neo4j simply
  graphname=$(basename $dbaddress) && \
  printf "Purging the database @ graphname\n"
  rm -rf /data/databases/$graphname
  rm -rf /data/transactions/$graphname
  cp -r /data-init/databases/neo4j/. /data/databases/$graphname
  cp -r /data-init/transactions/neo4j/. /data/transactions/$graphname
service neo4j start

source {{pipeline.neo4j.scripts_path}}/wait-for-db.sh

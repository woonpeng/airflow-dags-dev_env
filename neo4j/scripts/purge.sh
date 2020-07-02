#!/bin/bash
set -euo pipefail

service neo4j stop && \
  queryresult=$(neo4j-admin check-consistency --database=neo4j | grep "record format from store") && \
  regex='record format from store (.*)' && \
  [[ $queryresult =~ $regex ]] && \
  dbaddress=${BASH_REMATCH[1]} && \
  printf "Purging the database\n"
  rm -rf $dbaddress
service neo4j start

source {{pipeline.neo4j.scripts_path}}/wait-for-db.sh

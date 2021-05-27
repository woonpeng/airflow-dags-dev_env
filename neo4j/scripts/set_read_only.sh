#!/bin/bash
set -euo pipefail

# defaults
if [[ -z $graphname ]]; then
  echo "<graphname> must be specified in environment variables or as a parameter" >&2
  exit 2
fi

service neo4j stop && \
  CONFIG_FILE="$NEO4J_HOME/conf/neo4j.conf" && \
  sed -i "/dbms.read_only/c\\dbms.read_only=${1:-true}" $CONFIG_FILE
service neo4j start

source wait-for-db.sh -p $boltport

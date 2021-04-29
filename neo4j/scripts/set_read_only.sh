#!/bin/bash
set -euo pipefail

# defaults
graphname=${NEO4J_GRAPHNAME:-}

while [[ "$#" -gt 1 ]]; do
  case $1 in
    -g|--graphname)
      shift
      if [ "$#" -gt 0 ] && [ ${1:0:1} != "-" ]; then
        graphname=$1
        shift
      else
        echo "Error: Argument for <graphname> is missing" >&2
        exit 2
      fi
      ;;
  esac
done

if [[ -z $graphname ]]; then
  echo "<graphname> must be specified in environment variables or as a parameter" >&2
  exit 2
fi

# get bolt port from query
boltport=7687
queryresult=$(service neo4j status | grep "Bolt enabled on") && \
  regex='Bolt enabled on 0.0.0.0:(.*).' && \
  [[ $queryresult =~ $regex ]] && \
  boltport=${BASH_REMATCH[1]}

service neo4j stop && \
  CONFIG_FILE="$NEO4J_HOME/conf/neo4j.conf" && \
  sed -i "/dbms.read_only/c\\dbms.read_only=${1:-true}" $CONFIG_FILE
service neo4j start

source wait-for-db.sh -p $boltport

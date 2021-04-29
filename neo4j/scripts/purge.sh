#!/bin/bash
set -euo pipefail

# defaults
if [[ -z $graphname ]]; then
  echo "<graphname> must be specified in environment variables or as a parameter" >&2
  exit 2
fi
if [[ -z $blankdbpath ]]; then
  echo "<blankdbpath> must be specified in environment variables or as a parameter" >&2
  exit 2
fi

# read-only check
if [[ $curr_read_only == true || $curr_read_only == True || $curr_read_only == 1 ]]; then
  echo "Set dbms.read_only to false to run this command" >&2
  exit 2
fi

service neo4j stop && \
  queryresult=$(neo4j-admin check-consistency --database=$graphname | grep "record format from store") && \
  regex='record format from store (.*)' && \
  [[ $queryresult =~ $regex ]] && \
  dbaddress=${BASH_REMATCH[1]} && \
  dbbase=$(dirname "$(dirname $dbaddress)") && \
  echo "Purging the database @ $graphname" && \
  rm -r $dbbase/databases/$graphname && \
  cp -r $blankdbpath/databases/neo4j/. $dbbase/databases/$graphname && \
  rm -r $dbbase/transactions/$graphname && \
  cp -r $blankdbpath/transactions/neo4j/. $dbbase/transactions/$graphname && \
  chown -R ${NEO4J_USER:-neo4j}:${NEO4J_USER:-neo4j} $dbbase/databases/$graphname && \
  chown -R ${NEO4J_USER:-neo4j}:${NEO4J_USER:-neo4j} $dbbase/transactions/$graphname
service neo4j start

source wait-for-db.sh -p $boltport

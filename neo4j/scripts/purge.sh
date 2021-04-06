#!/bin/bash
set -euo pipefail

# defaults
graphname=${NEO4J_GRAPHNAME:-}
blankdbpath=${NEO4J_BLANKDBPATH:-}

while [[ "$#" -gt 0 ]]; do
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
    -b|--blankdbpath)
    shift
      if [ "$#" -gt 0 ] && [ ${1:0:1} != "-" ]; then
        blankdbpath=$1
        shift
      else
        echo "Error: Argument for <blankdbpath> is missing" >&2
        exit 2
      fi
      ;;
    *) # ignore otherwise
      shift
      ;;
  esac
done

if [[ -z $graphname ]]; then
  echo "<graphname> must be specified in environment variables or as a parameter" >&2
  exit 2
fi
if [[ -z $blankdbpath ]]; then
  echo "<blankdbpath> must be specified in environment variables or as a parameter" >&2
  exit 2
fi

# get bolt port from query
boltport=7687
queryresult=$(service neo4j status | grep "Bolt enabled on") && \
  regex='Bolt enabled on 0.0.0.0:(.*).' && \
  [[ $queryresult =~ $regex ]] && \
  boltport=${BASH_REMATCH[1]}

service neo4j stop && \
  queryresult=$(path=$graphname | grep "record format from store") && \
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

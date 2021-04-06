#!/bin/bash
set -euo pipefail

# defaults
graphname=${NEO4J_GRAPHNAME:-}
backuppath=${NEO4J_BACKUPPATH:-}
BACKUP_FILENAME=""

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
    -b|--backuppath)
      shift
      if [ "$#" -gt 0 ] && [ ${1:0:1} != "-" ]; then
        backuppath=$1
        shift
      else
        echo "Error: Argument for <backuppath> is missing" >&2
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
if [[ -z $backuppath ]]; then
  echo "<backuppath> must be specified in environment variables or as a parameter" >&2
  exit 2
fi

# get bolt port from query
boltport=7687
queryresult=$(service neo4j status | grep "Bolt enabled on") && \
  regex='Bolt enabled on 0.0.0.0:(.*).' && \
  [[ $queryresult =~ $regex ]] && \
  boltport=${BASH_REMATCH[1]}

mkdir -p $backuppath && \
service neo4j stop && \
  queryresult=$(neo4j-admin check-consistency --database=$graphname | grep "record format from store") && \
  regex='record format from store (.*)' && \
  [[ $queryresult =~ $regex ]] && \
  dbaddress=${BASH_REMATCH[1]} && \
  dbbase=$(dirname "$(dirname $dbaddress)")&& \
  pushd $backuppath && \
    if [ $# -eq 0 ] || [ -z "$1" ] ; then \
      BACKUP_FILENAME=$(ls -rc | tail -n 1)
      printf "Restoring latest backup from $BACKUP_FILENAME\n"
    else \
      RAW=$1
      BACKUP_FILENAME="${RAW//\//_}".dump
      printf "Restoring backup from $BACKUP_FILENAME\n"
    fi && \
    neo4j-admin load --database="$graphname" --from="$BACKUP_FILENAME" --force && \
  popd && \
  chown -R ${NEO4J_USER:-neo4j}:${NEO4J_USER:-neo4j} $dbbase/databases/$graphname && \
  chown -R ${NEO4J_USER:-neo4j}:${NEO4J_USER:-neo4j} $dbbase/transactions/$graphname
service neo4j start

source wait-for-db.sh -p $boltport

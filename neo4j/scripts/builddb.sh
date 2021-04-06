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
    *) # ignore otherwise
      shift
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
  queryresult=$(neo4j-admin check-consistency --database=$graphname | grep "record format from store") && \
  regex='record format from store (.*)' && \
  [[ $queryresult =~ $regex ]] && \
  dbaddress=${BASH_REMATCH[1]} && \
  dbbase=$(dirname "$(dirname $dbaddress)")&& \
  pushd $dbbase && \
    if [ $# -eq 0 ] || [ -z "$1" ] ; then \
      exit_status=-1
    else \
      RAW=$1
      if [ -e $RAW ]; then
        tmpdir=$(mktemp -d)
        unzip $RAW -d $tmpdir

        argstr=""
        for fn in $tmpdir/*.csv; do
          if [[ "$(head -n 1 $fn)" == *":START_ID"* ]]; then
            argstr=$argstr" --relationships $fn "
          fi
          if [[ "$(head -n 1 $fn)" == *":ID"* ]]; then
            argstr=$argstr" --nodes $fn "
          fi
        done

        printf "Importing database from $RAW\n"
        neo4j-admin import --database=$graphname --multiline-fields=true --skip-duplicate-nodes=true $argstr
        rm -rf $tmpdir
      fi

    fi && \
  popd && \
  chown -R ${NEO4J_USER:-neo4j}:${NEO4J_USER:-neo4j} $dbbase/databases/$graphname && \
  chown -R ${NEO4J_USER:-neo4j}:${NEO4J_USER:-neo4j} $dbbase/transactions/$graphname
service neo4j start

source wait-for-db.sh -p $boltport



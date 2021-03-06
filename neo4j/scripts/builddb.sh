#!/bin/bash
set -euo pipefail

service neo4j stop && \
  queryresult=$(neo4j-admin check-consistency --database=neo4j | grep "record format from store") && \
  regex='record format from store (.*)' && \
  [[ $queryresult =~ $regex ]] && \
  dbaddress=${BASH_REMATCH[1]} && \
  graphname=$(basename $dbaddress) && \
  dbbase=$(dirname $dbaddress) && \
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
  popd
service neo4j start

source {{pipeline.neo4j.scripts_path}}/wait-for-db.sh



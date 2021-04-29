#!/bin/bash
set -euo pipefail

# defaults
if [[ -z $graphname ]]; then
  echo "<graphname> must be specified in environment variables or as a parameter" >&2
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
        for fn in $tmpdir/nodes/*.{csv,csv.gz}; do
          if [ -e $fn ]; then
              argstr=$argstr" --nodes $fn "
          fi
        done
        for fn in $tmpdir/relationships/*.{csv,csv.gz}; do
          if [ -e $fn ]; then
              argstr=$argstr" --relationships $fn "
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



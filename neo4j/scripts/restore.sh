#!/bin/bash
set -euo pipefail

BACKUP_FILENAME=""

mkdir -p {{pipeline.neo4j.backup_path}} && \
sudo service neo4j stop && \
  pushd {{pipeline.neo4j.backup_path}} && \
    if [ $# -eq 0 ] || [ -z "$1" ] ; then \
      BACKUP_FILENAME=$(ls -rc | tail -n 1)
      printf "Restoring latest backup from $BACKUP_FILENAME\n"
    else \
      RAW=$1
      BACKUP_FILENAME="${RAW//\//_}".dump
      printf "Restoring backup from $BACKUP_FILENAME\n"
    fi && \
    neo4j-admin load --database=graph.db --from="$BACKUP_FILENAME" --force=true && \
  popd
sudo service neo4j start

source {{pipeline.neo4j.scripts_path}}/wait-for-db.sh

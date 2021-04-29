#!/bin/bash
set -euo pipefail

function help {
  echo "Usage: manager.sh [command] [arguments]"
  echo "---------------------------------------"
  echo "[command]"
  echo "backup:   Backup the Neo4j database"
  echo "restore:  Restore the specified or latest (if unspecified) Neo4j database"
  echo "builddb:  Build Neo4j database from a zip file containing the node and edges csv"
  echo "cleanup:  Clean up the backups that are older than 30 days"
  echo "purge:    Purge the Neo4j database"
  echo "set_read_only: Set database read only or not"
}

if [ $# -gt 0 ]; then

  if [ "$1" == "backup" ]; then
    backup.sh "${@:2}"
  elif [ "$1" == "restore" ]; then
    restore.sh "${@:2}"
  elif [ "$1" == "builddb" ]; then
    purge.sh "${@:2}"
    builddb.sh "${@:2}"
  elif [ "$1" == "cleanup" ]; then
    cleanup.sh "${@:2}"
  elif [ "$1" == "purge" ]; then
    purge.sh "${@:2}"
  elif [ "$1" == "set_read_only" ]; then
    set_read_only.sh "${@:2}"
  else
    help
  fi

else

  help

fi


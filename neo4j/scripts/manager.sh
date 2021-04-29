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

# Check read-only setting
curr_read_only=true
CONFIG_FILE="$NEO4J_HOME/conf/neo4j.conf" && \
  queryresult=$(cat $CONFIG_FILE | grep "dbms.read_only") && \
  regex='dbms.read_only=(.*)' && \
  [[ $queryresult =~ $regex ]] && \
  curr_read_only=${BASH_REMATCH[1]}
echo "Current dbms.read_only=$curr_read_only"
export curr_read_only=$curr_read_only

# get bolt port from query
boltport=7687
queryresult=$(service neo4j status | grep "Bolt enabled on") && \
  regex='Bolt enabled on 0.0.0.0:(.*).' && \
  [[ $queryresult =~ $regex ]] && \
  boltport=${BASH_REMATCH[1]}
export boltport=$boltport

if [ $# -gt 0 ]; then
  # parse command
  command="$1"
  shift

  # parse arguments and export
  graphname=${NEO4J_GRAPHNAME:-}
  blankdbpath=${NEO4J_BLANKDBPATH:-}
  backuppath=${NEO4J_BACKUPPATH:-}

  while [[ "$#" -gt 0 && ${1:0:1} == "-" ]]; do
    case $1 in
      --graphname)
        shift
        if [ "$#" -gt 0 ] && [ ${1:0:1} != "-" ]; then
          graphname=$1
          shift
        else
          echo "Error: Argument for <graphname> is missing" >&2
          exit 2
        fi
        ;;
      --blankdbpath)
      shift
        if [ "$#" -gt 0 ] && [ ${1:0:1} != "-" ]; then
          blankdbpath=$1
          shift
        else
          echo "Error: Argument for <blankdbpath> is missing" >&2
          exit 2
        fi
        ;;
      --backuppath)
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

  export graphname=$graphname
  export blankdbpath=$blankdbpath
  export backuppath=$backuppath

  # run commnands
  if [ "$command" == "backup" ]; then
    backup.sh "${@:1}"
  elif [ "$command" == "restore" ]; then
    restore.sh "${@:1}"
  elif [ "$command" == "builddb" ]; then
    purge.sh "${@:1}" && \
    builddb.sh "${@:1}"
  elif [ "$command" == "cleanup" ]; then
    cleanup.sh "${@:1}"
  elif [ "$command" == "purge" ]; then
    purge.sh "${@:1}"
  elif [ "$command" == "set_read_only" ]; then
    if [ "$curr_read_only" != "${1:-true}" ]; then set_read_only.sh "${@:1}"; fi
  else
    help
  fi

else

  help

fi


#!/bin/bash
set -euo pipefail

# defaults
MAX_DAYS=30
backuppath=${NEO4J_BACKUPPATH:-}

while [[ "$#" -gt 0 ]]; do
  case $1 in
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

if [[ -z $backuppath ]]; then
  echo "<backuppath> must be specified in environment variables or as a parameter" >&2
  exit 2
fi

printf "Cleanup backup files older than $MAX_DAYS days\n"
find $backuppath -name '*.dump' -type f -mtime "$MAX_DAYS" -delete

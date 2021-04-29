#!/bin/bash
set -euo pipefail

# defaults
MAX_DAYS=30

if [[ -z $backuppath ]]; then
  echo "<backuppath> must be specified in environment variables or as a parameter" >&2
  exit 2
fi

printf "Cleanup backup files older than $MAX_DAYS days\n"
find $backuppath -name '*.dump' -type f -mtime "$MAX_DAYS" -delete

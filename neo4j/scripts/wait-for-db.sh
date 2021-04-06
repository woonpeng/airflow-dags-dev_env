#!/bin/bash
set -euo pipefail

# defaults
port=7687

while [[ "$#" -gt 1 ]]; do
  case $1 in
    -p|--port)
      shift
      if [ "$#" -gt 0 ] && [ ${1:0:1} != "-" ]; then
        port=$1
        shift
      else
        echo "Error: Argument for <port> is missing" >&2
        exit 2
      fi
      ;;
    *) # ignore otherwise
      shift
      ;;
  esac
done

# check to see if timeout is from busybox?
TIMEOUT_PATH=$(realpath $(which timeout))
if [[ $TIMEOUT_PATH =~ "busybox" ]]; then
        ISBUSY=1
        BUSYTIMEFLAG="-t"
else
        ISBUSY=0
        BUSYTIMEFLAG=""
fi

# Wait for neo4j to start by trying to connect to the neo4j address and port
# The official neo4j image starts neo4j once to check for correct credentials
# stops it, and then starts it for real.
NEO4J_TIMEOUT=60
DATABASE_DEV="/dev/tcp/localhost/$port"
echo "Checking database connection ${DATABASE_DEV}"
timeout ${BUSYTIMEFLAG} ${NEO4J_TIMEOUT} bash <<EOT

neo4j_wait() {
  while ! (echo > "${DATABASE_DEV}") >/dev/null 2>&1; do
      echo "Waiting for database ${DATABASE_DEV}"
      sleep 2;
  done;

  sleep 5
}

neo4j_wait
neo4j_wait
EOT
RESULT=$?

if [ ${RESULT} -eq 0 ]; then
    echo "Neo4J database now available"
else
    echo "Neo4J database is not available"
    exit 1
fi

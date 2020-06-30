#!/usr/bin/env bash

# Find out tmp dir
tmpdir=$(dirname $(mktemp -u))

case $SSH_ORIGINAL_COMMAND in
  'manager.sh'*)
    exec $SSH_ORIGINAL_COMMAND
    ;;
  'scp'*)
    exec $SSH_ORIGINAL_COMMAND
    ;;
  'mktemp'*)
    exec $SSH_ORIGINAL_COMMAND
    ;;
  "rm -r $tmpdir/"*)
    REGEX="rm -r (.*)"
    [[ $SSH_ORIGINAL_COMMAND =~ $REGEX ]]
    RMPATH=${BASH_REMATCH[1]}
    RMPATH=$(readlink -f ${RMPATH})
    if [[ "$RMPATH" =~ ^$tmpdir/.* ]];
    then
      exec $SSH_ORIGINAL_COMMAND
    else
      echo "Access Denied"
    fi
    ;;
  *)
    echo "Access Denied"
    ;;
esac



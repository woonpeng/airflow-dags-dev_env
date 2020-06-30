#!/usr/bin/env bash
tempfile=$(mktemp)
filelist=$(find $1 ! -name $0)
for file in $filelist
do
  echo Updating $file
  j2 $file "${EXTENSION_INSTALL_PATH}/config.json" > $tempfile
  cat $tempfile > $file
  dos2unix $file
  rm $tempfile
done

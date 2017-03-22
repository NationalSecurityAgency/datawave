#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

. ../ingest/ingest-env.sh

for (( count=0; count < ${#POLLER_DATA_TYPES[@]}; count=$((count + 1)) )); do
  if [[ "$#" == "0" || "$@" =~  "${POLLER_DATA_TYPES[$count]}" ]]; then
     find ${POLLER_OUTPUT_DIRECTORIES[$count]}/errors -type f -exec mv \{\} ${POLLER_INPUT_DIRECTORIES[$count]} \;
  fi
done

if [[ "$#" == 0 || "$@" =~ "$ERROR_POLLER_DATA_TYPE" ]]; then
  OLD_IFS="$IFS"
  IFS=","
  ERROR_INPUT_DIRECTORIES=( $ERROR_POLLER_INPUT_DIRECTORIES )
  ERROR_OUTPUT_DIRECTORIES=( $ERROR_POLLER_OUTPUT_DIRECTORIES )
  IFS="$OLD_IFS"

  for (( count=0; count < ${#ERROR_OUTPUT_DIRECTORIES[@]}; count=$((count + 1)) )); do
     find ${ERROR_OUTPUT_DIRECTORIES[$count]}/errors -type f -exec mv \{\} ${ERROR_INPUT_DIRECTORIES[$count]} \;
  done
fi


#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
    THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
    MKTEMP="mktemp -t `basename $0`"
else
    THIS_SCRIPT=`readlink -f $0`
    MKTEMP="mktemp -t `basename $0`.XXXXXXXX"
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

FORCE=true
. ../ingest/ingest-env.sh

export INGEST_BIN=$THIS_DIR/..

# now apply the appropriate system configuration
if [[ ${#STAGING_HOSTS[@]} == 1 && "${STAGING_HOSTS[0]}" == "localhost" ]]; then

  for (( count=0; count < ${#POLLER_DATA_TYPES[@]}; count=$((count + 1)) )); do
      if [[ "$#" == "0" || "$@" =~  "${POLLER_DATA_TYPES[$count]}" ]]; then
          $THIS_DIR/../poller/reset-failures.sh ${POLLER_DATA_TYPES[$count]}
      fi
  done

  if [[ "$#" == 0 || "$@" =~ "$ERROR_POLLER_DATA_TYPE" ]]; then
    $THIS_DIR/../poller/reset-failures.sh $ERROR_POLLER_DATA_TYPE
  fi

else

  stagingHosts=`$MKTEMP`
  trap 'rm -f "$stagingHosts"; exit $?' INT TERM EXIT
  for host in ${STAGING_HOSTS[@]}; do
      echo $host >> $stagingHosts
  done

  for (( count=0; count < ${#POLLER_DATA_TYPES[@]}; count=$((count + 1)) )); do
      if [[ "$#" == "0" || "$@" =~  "${POLLER_DATA_TYPES[$count]}" ]]; then
          pssh -t 0 -p 25 -o /tmp/stdout -e /tmp/stderr -h ${stagingHosts} "$THIS_DIR/../poller/reset-failures.sh ${POLLER_DATA_TYPES[$count]}" < /dev/null
      fi
  done

  if [[ "$#" == 0 || "$@" =~ "$ERROR_POLLER_DATA_TYPE" ]]; then
    for (( count=0; count < ${#ERROR_OUTPUT_DIRECTORIES[@]}; count=$((count + 1)) )); do
       pssh -t 0 -p 25 -o /tmp/stdout -e /tmp/stderr -h ${stagingHosts} "$THIS_DIR/../poller/reset-failures.sh ${POLLER_DATA_TYPES[$count]}" < /dev/null
    done
  fi

  rm $stagingHosts
  trap - INT TERM EXIT

fi

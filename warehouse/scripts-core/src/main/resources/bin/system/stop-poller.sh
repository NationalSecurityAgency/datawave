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
#stop scripts do not require force despite lock files
. ../ingest/ingest-env.sh -force

export INGEST_BIN=$THIS_DIR/..

# now apply the appropriate system configuration
if [[ ${#STAGING_HOSTS[@]} == 1 && "${STAGING_HOSTS[0]}" == "localhost" ]]; then

  for (( count=0; count < ${#POLLER_DATA_TYPES[*]}; count=$((count + 1)) )); do
      $INGEST_BIN/poller/stopEventPoller.sh ${POLLER_DATA_TYPES[$count]} $@
  done

  if [ -n "${ERROR_POLLER_DATA_TYPE}" ]; then
      $INGEST_BIN/poller/stopEventPoller.sh ${ERROR_POLLER_DATA_TYPE} $@
  fi

else

  stagingHosts=`$MKTEMP`
  trap 'rm -f "$stagingHosts"; exit $?' INT TERM EXIT
  for host in ${STAGING_HOSTS[@]}; do
      echo $host >> $stagingHosts
  done

  for (( count=0; count < ${#POLLER_DATA_TYPES[*]}; count=$((count + 1)) )); do
      pssh -p 25 -o /tmp/stdout -e /tmp/stderr -h ${stagingHosts} "$INGEST_BIN/poller/stopEventPoller.sh ${POLLER_DATA_TYPES[$count]} $@" < /dev/null
  done

  if [ -n "${ERROR_POLLER_DATA_TYPE}" ]; then
      pssh -p 25 -o /tmp/stdout -e /tmp/stderr -h ${stagingHosts} "$INGEST_BIN/poller/stopEventPoller.sh ${ERROR_POLLER_DATA_TYPE} $@" < /dev/null
  fi

  rm $stagingHosts
  trap - INT TERM EXIT

fi

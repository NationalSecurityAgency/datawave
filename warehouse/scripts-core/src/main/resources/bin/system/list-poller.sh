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

  $INGEST_BIN/poller/listPoller.sh

else

  stagingHosts=`$MKTEMP`
  trap 'rm -f "$stagingHosts"; exit $?' INT TERM EXIT
  for host in ${STAGING_HOSTS[@]}; do
      echo $host >> $stagingHosts
  done

  pdsh -p 1 -i -h $stagingHosts "$INGEST_BIN/poller/listPoller.sh" < /dev/null | grep -v 'SUCCESS'

  rm $stagingHosts
  trap - INT TERM EXIT

fi

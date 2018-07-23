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

export METRICS_BIN=$THIS_DIR/..

# now apply the appropriate system configuration
if [[ "$INGEST_HOST" == "localhost" || "$INGEST_HOST" == `hostname` || "$INGEST_HOST" == `hostname -s` ]]; then

  $METRICS_BIN/metrics/stopMetricsIngest.sh $@

else

  ingestHost=`$MKTEMP`
  trap 'rm -f "$ingestHost"; exit $?' INT TERM EXIT
  echo $INGEST_HOST > $ingestHost

  pdsh -f 25 -w ^${ingestHost} "$METRICS_BIN/metrics/stopMetricsIngest.sh $@" < /dev/null

  rm $ingestHost
  trap - INT TERM EXIT

fi


if [[ ${#STAGING_HOSTS[@]} == 1 && "${STAGING_HOSTS[0]}" == "localhost" ]]; then

  $METRICS_BIN/metrics/stopMetricsIngest.sh $@

else
  
  stagingHosts=`$MKTEMP`
  trap 'rm -f "$stagingHosts"; exit $?' INT TERM EXIT
  for host in ${STAGING_HOSTS[@]}; do
      echo $host >> $stagingHosts
  done

  pdsh -f 25 -w ^${stagingHosts} "$METRICS_BIN/metrics/stopMetricsIngest.sh $@" < /dev/null

  rm $stagingHosts
  trap - INT TERM EXIT

fi

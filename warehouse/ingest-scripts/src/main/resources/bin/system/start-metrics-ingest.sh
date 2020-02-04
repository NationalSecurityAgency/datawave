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
script_name=$(basename ${0})

. ../ingest/ingest-env.sh
. ../util/logging_pdsh.sh

export METRICS_BIN=$THIS_DIR/..

FORCE="-force"
if [[ "$@" =~ ".*-cron.*" || "$@" =~ "-cron" ]]; then
   FORCE="";
fi


# now apply the appropriate system configuration
if [[ "$INGEST_HOST" == "localhost" || "$INGEST_HOST" == `hostname` || "$INGEST_HOST" == `hostname -s` ]]; then

  $METRICS_BIN/metrics/startMetricsIngest.sh ingest $FORCE
  $METRICS_BIN/metrics/startMetricsIngest.sh loader $FORCE
  $METRICS_BIN/metrics/startMetricsIngest.sh flagmaker $FORCE

else

  ingestHost=`$MKTEMP`
  trap 'rm -f "$ingestHost"; exit $?' INT TERM EXIT
  echo $INGEST_HOST > $ingestHost

  logging_pdsh "${script_name}" -cmd ingest_cmd \
    -f 25 -w ^${ingestHost} "$METRICS_BIN/metrics/startMetricsIngest.sh ingest $FORCE"
  logging_pdsh "${script_name}" -cmd loader_cmd \
    -f 25 -w ^${ingestHost} "$METRICS_BIN/metrics/startMetricsIngest.sh loader $FORCE"
  logging_pdsh "${script_name}" -cmd flagmaker_cmd \
    -f 25 -w ^${ingestHost} "$METRICS_BIN/metrics/startMetricsIngest.sh flagmaker $FORCE"

  rm $ingestHost
  trap - INT TERM EXIT

fi


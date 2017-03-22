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
. ../ingest/ingest-env.sh

FORCE="-force"
if [[ "$@" =~ ".*-cron.*" || "$@" =~ "-cron" ]]; then
   FORCE="";
fi

port=$POLLER_JMX_PORT_START
declare -a JMX_OPTS
for (( count=0; count < ${#POLLER_DATA_TYPES[@]}; count=$((count + 1)) )); do
  JMX_OPTS[count]="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=true -Dcom.sun.management.jmxremove.ssl=false -Dcom.sun.management.jmxremote.port=$port "
  port=$((port+1))
done


export INGEST_BIN=$THIS_DIR/..

# now apply the appropriate system configuration
if [[ ${#STAGING_HOSTS[@]} == 1 && "${STAGING_HOSTS[0]}" == "localhost" ]]; then

  for (( count=0; count < ${#POLLER_DATA_TYPES[@]}; count=$((count + 1)) )); do
      $INGEST_BIN/poller/startEventPoller.sh ${POLLER_INPUT_DIRECTORIES[$count]} ${POLLER_DATA_TYPES[$count]} ${POLLER_OUTPUT_DIRECTORIES[$count]} ${POLLER_CLIENT_OPTS[$count]} ${JMX_OPTS[$count]} $FORCE
  done

else
  
  stagingHosts=`$MKTEMP`
  trap 'rm -f "$stagingHosts"; exit $?' INT TERM EXIT
  for host in ${STAGING_HOSTS[@]}; do
      echo $host >> $stagingHosts
  done

  for (( count=0; count < ${#POLLER_DATA_TYPES[@]}; count=$((count + 1)) )); do
      pssh -p 25 -o /tmp/stdout -e /tmp/stderr -h ${stagingHosts} "$INGEST_BIN/poller/startEventPoller.sh ${POLLER_INPUT_DIRECTORIES[$count]} ${POLLER_DATA_TYPES[$count]} ${POLLER_OUTPUT_DIRECTORIES[$count]} ${POLLER_CLIENT_OPTS[$count]} ${JMX_OPTS[$count]} $FORCE" < /dev/null
  done

  rm $stagingHosts
  trap - INT TERM EXIT

fi


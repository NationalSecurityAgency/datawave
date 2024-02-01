#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

. ./ingest-env.sh
. ./ingest-libs.sh

HADOOP_NATIVE_LIB_DIR="$INGEST_HADOOP_HOME/lib/native"
CLASSPATH=$CLASSPATH:$($HADOOP_HOME/bin/hadoop classpath)
CLASSPATH=$CLASSPATH:../../config/log4j-flagmaker.xml
JAVA_OPTS="${JAVA_OPTS} -Djava.library.path=${HADOOP_NATIVE_LIB_DIR}"
export HADOOP_CLASSPATH=$CLASSPATH

start() {
  for (( count=0; count < ${#FLAG_MAKER_CONFIG[@]}; count=$((count + 1)) )); do
    config=${FLAG_MAKER_CONFIG[$count]}
    config_base=$(basename ${config} .xml)
    if [[ "$config" != "" ]]; then
      if pgrep -f "Dapp=FlagMaker -DappConfig=$config" 2>&1 >/dev/null ; then
        echo "FlagMaker for $config already running"
      else
        $JAVA_HOME/bin/java -Dlog4j.configuration=log4j-flagmaker.xml -Dapp=FlagMaker -DappConfig=$config $JAVA_OPTS -cp $CLASSPATH datawave.util.flag.FlagMaker -flagConfig $config ${FLAG_EXTRA_ARGS} $1>> ${LOG_DIR}/flag_maker_${config_base}.log 2>&1 < /dev/null &
      fi
    fi
  done
}

stop() {
  for (( count=0; count < ${#FLAG_MAKER_CONFIG[@]}; count=$((count + 1)) )); do
    config=${FLAG_MAKER_CONFIG[$count]}
    config_base=$(basename ${config} .xml)
    if [[ "$config" != "" ]]; then
      if ! pgrep -f "Dapp=FlagMaker -DappConfig=$config" 2>&1 >/dev/null ; then
        echo "FlagMaker for $config not running"
      else
        $JAVA_HOME/bin/java -Dlog4j.configuration=log4j-flagmaker.xml $JAVA_OPTS -cp $CLASSPATH datawave.util.flag.FlagMaker -flagConfig $config -shutdown >> ${LOG_DIR}/flag_maker_${config_base}.log 2>&1 < /dev/null &
      fi
    fi
  done
  echo "Waiting for FlagMakers to shut down (Ctrl+c to cancel immediately)"
  PID=$(pgrep -f "app=FlagMaker" 2>/dev/null) 
  while kill -0 $PID >/dev/null 2>&1
  do
    sleep 1
    PID=$(pgrep -f "app=FlagMaker" 2>/dev/null) 
  done;
  echo "Flag Maker shut down"
}

case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  *)
    echo $"Usage: $0 {start|stop}"
    exit 1
esac


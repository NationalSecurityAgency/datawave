#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

if [[ -z $DATAWAVE_INGEST_HOME ]]; then
  export DATAWAVE_INGEST_HOME=$THIS_DIR/../..
fi

INCLUDE_POLLER_LIBS="true"
. ../ingest/ingest-libs.sh

CONF_FILE="$THIS_DIR/../../config/log4j-poller.xml"

export HADOOP_CLASSPATH=$CLASSPATH
HADOOP_OPTS="$JAVA_OPTS -Dlog4j.configuration=file://$CONF_FILE"

# cache the options
opts=$@

extractArg (){
  flag="$1"
  option="unset"
  while [[ $# -gt 0 ]]; do
	shift
        arg="$1"
        if [[ "$arg" == "$flag" ]]; then
	    shift
	    option="$1"
	    break
        fi
  done
  echo $option
}

#
# Set -Ddatatype, -DinDir, -DoutDir markers
#
type=$(extractArg -t $opts)
inDir=$(extractArg -d $opts)
outDir=$(extractArg -w $opts)

#
# If any of the extra opts start with -D, then add them to HADOOP_OPTS and remove them from extra opts
#
declare -a extra_opts
idx=0
for arg in $opts; do
        if [[ $arg =~ ^-D.*$ ]]; then
                HADOOP_OPTS="$HADOOP_OPTS $arg"
        else
                extra_opts[$idx]="$arg"
                idx=$((idx+1))
        fi
done

export HADOOP_OPTS=" -Dapp=datawavePoller -Ddatatype=$type -DinDir=$inDir -DoutDir=$outDir $HADOOP_OPTS "
echo "HADOOP_OPTS = $HADOOP_OPTS"
echo $INGEST_HADOOP_HOME/bin/hadoop --config $INGEST_HADOOP_CONF jar $POLLER_JAR nsa.datawave.poller.Poller ${extra_opts[@]}
$INGEST_HADOOP_HOME/bin/hadoop --config $INGEST_HADOOP_CONF jar $POLLER_JAR nsa.datawave.poller.Poller ${extra_opts[@]}

#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

# Get lib versions
. ../ingest/ingest-libs.sh

EXTRA_ARGS=$@

#
# Capture only the ingest config files
#
declare -a INGEST_CONFIG
i=0
for f in ../../config/*-config.xml; do
  INGEST_CONFIG[i++]=$(basename $f)
done

#
# Configuration
#
WORKDIR=${BASE_WORK_DIR}/
PATTERN="'*'"

if [[ -e "$INGEST_HADOOP_HOME/share/hadoop/tools/lib/hadoop-$HADOOP_VERSION-distcp.jar" ]]; then
    # standard naming
    export HADOOP_CLASSPATH=$CLASSPATH:$INGEST_HADOOP_HOME/share/hadoop/tools/lib/hadoop-$HADOOP_VERSION-distcp.jar
else
    # cloudera naming
    export HADOOP_CLASSPATH=$CLASSPATH:$INGEST_HADOOP_HOME/share/hadoop/tools/lib/hadoop-distcp-$HADOOP_VERSION.jar
fi

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
shutdownPort=$(extractArg -shutdownPort $opts)

OBSERVER_OPTS=
if [[ ! -z $JOB_OBSERVERS ]]; then
  OBSERVER_OPTS="-jobObservers $JOB_OBSERVERS $JOB_OBSERVER_EXTRA_OPTS"
fi

export HADOOP_OPTS=" ${HADOOP_INGEST_OPTS} -Dapp=bulkIngestMapFileLoader -DshutdownPort=$shutdownPort -Dfile.encoding=UTF8 -Duser.timezone=GMT"
$MAP_FILE_LOADER_COMMAND_PREFIX $INGEST_HADOOP_HOME/bin/hadoop --config $WAREHOUSE_HADOOP_CONF jar ${DATAWAVE_INGEST_CORE_JAR} datawave.ingest.mapreduce.job.BulkIngestMapFileLoader $WORKDIR $PATTERN $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS $USERNAME $PASSWORD -majcThreshold ${MAP_LOADER_MAJC_THRESHOLD} -sleepTime 5000 -numHdfsThreads 100 -numThreads 20 -majcCheckInterval 1 -maxDirectories 200 -numAssignThreads 60 -seqFileHdfs $INGEST_HDFS_NAME_NODE -srcHdfs $WAREHOUSE_HDFS_NAME_NODE -destHdfs $WAREHOUSE_HDFS_NAME_NODE -jt $WAREHOUSE_JOBTRACKER_NODE $OBSERVER_OPTS $MAP_FILE_LOADER_EXTRA_ARGS $EXTRA_ARGS ${INGEST_CONFIG[@]}
RETURN_CODE=$?

exit $RETURN_CODE

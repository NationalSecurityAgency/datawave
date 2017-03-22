#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
	THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
	THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

. ../ingest/ingest-env.sh

cd ../../lib
CLASSPATH=$INGEST_ACCUMULO_HOME/lib/accumulo-core.jar
CLASSPATH=${CLASSPATH}:$INGEST_ACCUMULO_HOME/lib/accumulo-start.jar
CLASSPATH=${CLASSPATH}:$INGEST_ACCUMULO_HOME/lib/accumulo-server-base.jar
CLASSPATH=${CLASSPATH}:$WAREHOUSE_ACCUMULO_HOME/lib/accumulo-fate.jar
CLASSPATH=${CLASSPATH}:$INGEST_ACCUMULO_HOME/lib/accumulo-trace.jar
CLASSPATH=${CLASSPATH}:$INGEST_ACCUMULO_HOME/lib/libthrift.jar
CLASSPATH=${CLASSPATH}:$ZOOKEEPER_HOME/zookeeper-$ZOOKEEPER_VERSION.jar
CLASSPATH=$CLASSPATH:`$INGEST_HADOOP_HOME/bin/hadoop classpath`

findJar (){
  ls -1 $1-[0-9]*.jar | sort | tail -1
}
addToCp () {
  CLASSPATH=$(findJar $1):${CLASSPATH}
}

addToCp datawave-poller
addToCp datawave-metrics-core

export CLASSPATH=$CLASSPATH

HADOOP_NATIVE_LIB_DIR="$INGEST_HADOOP_HOME/lib/native"

CONF_FILE="$PWD/../config/log4j-metrics.xml"
LOG_OPTS="-Dlog4j.configuration=file://$CONF_FILE"

$JAVA_HOME/bin/java -Dapp=pollerMetricsIngest $LOG_OPTS -Xmx2g -Djava.library.path=$HADOOP_NATIVE_LIB_DIR nsa.datawave.metrics.util.PollerMetricsIngester \
    -user $USERNAME -password $PASSWORD -instance $INGEST_INSTANCE_NAME -zookeepers $INGEST_ZOOKEEPERS -batchSize ${POLLER_METRICS_INGEST_BATCH_SIZE} $@

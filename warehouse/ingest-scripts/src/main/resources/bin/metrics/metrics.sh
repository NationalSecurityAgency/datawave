#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
  THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
  THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

. ../ingest/ingest-env.sh
. ../ingest/findJars.sh
#
# Get the classpath
#
CLASSPATH="../../config"
CLASSPATH=${DATAWAVE_METRICS_CORE_JAR}:${CLASSPATH}
CLASSPATH=${DATAWAVE_INGEST_CORE_JAR}:${CLASSPATH}
CLASSPATH=${COMMONS_CONFIGURATION_JAR}:${CLASSPATH}
CLASSPATH=${AC_HTRACE_JAR}:${CLASSPATH}
CLASSPATH=${CLASSPATH}:$WAREHOUSE_ACCUMULO_HOME/lib/accumulo-core.jar
CLASSPATH=${CLASSPATH}:$WAREHOUSE_ACCUMULO_HOME/lib/accumulo-start.jar
CLASSPATH=${CLASSPATH}:$WAREHOUSE_ACCUMULO_HOME/lib/accumulo-server-base.jar
CLASSPATH=${CLASSPATH}:$WAREHOUSE_ACCUMULO_HOME/lib/accumulo-fate.jar
CLASSPATH=${CLASSPATH}:$WAREHOUSE_ACCUMULO_HOME/lib/accumulo-trace.jar
CLASSPATH=${CLASSPATH}:$ZOOKEEPER_JAR

CLASSPATH=$(findJar gson):${CLASSPATH}
CLASSPATH=$(findJar libthrift):${CLASSPATH}
CLASSPATH=$(findJar datawave-ws-common-util):${CLASSPATH}

#
# Transform the classpath into a comma-separated list also
#
LIBJARS=`echo $CLASSPATH | sed 's/:/,/g'`

# cache the options
class=$1
opts=${@:1}

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
# Set -Dapp marker
#
type=$(extractArg -type $opts)
CHILD_OPTS="-Xmx2048m"

export HADOOP_CLASSPATH=$CLASSPATH

export HADOOP_OPTS=" -Dapp=${type}MetricsIngest -Dmapred.job.pool.name=bulkIngestQueue $HADOOP_OPTS"
$INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_METRICS_CORE_JAR} $class $HADOOP_OPTS -Dmapreduce.job.queuename=bulkIngestQueue -Dmapred.child.java.opts="$CHILD_OPTS" -Dmapred.max.split.size=800000 -libjars $LIBJARS -jt $INGEST_JOBTRACKER_NODE -fs $INGEST_HDFS_NAME_NODE $opts
RETURN_CODE=$?

exit $RETURN_CODE


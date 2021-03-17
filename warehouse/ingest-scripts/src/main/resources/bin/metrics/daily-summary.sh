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
CLASSPATH=${CLASSPATH}:$WAREHOUSE_ACCUMULO_HOME/lib/accumulo-core.jar
CLASSPATH=${CLASSPATH}:$WAREHOUSE_ACCUMULO_HOME/lib/accumulo-start.jar
CLASSPATH=${CLASSPATH}:$WAREHOUSE_ACCUMULO_HOME/lib/accumulo-server-base.jar
CLASSPATH=${CLASSPATH}:$WAREHOUSE_ACCUMULO_HOME/lib/accumulo-fate.jar
CLASSPATH=${CLASSPATH}:$WAREHOUSE_ACCUMULO_HOME/lib/accumulo-trace.jar
CLASSPATH=${CLASSPATH}:$ZOOKEEPER_JAR

CLASSPATH=$(findJar gson):${CLASSPATH}
CLASSPATH=$(findJar libthrift):${CLASSPATH}
CLASSPATH=$(findJar guava):${CLASSPATH}
CLASSPATH=$(findJar javatuples):${CLASSPATH}
CLASSPATH=$(findJar log4j):${CLASSPATH}
CLASSPATH=$(findJar datawave-core):${CLASSPATH}
CLASSPATH=$(findJar datawave-query-core):${CLASSPATH}

CLASSPATH=$(findJar datawave-metrics-core):${CLASSPATH}
CLASSPATH=$(findJar datawave-ingest-core):${CLASSPATH}
CLASSPATH=$(findJar datawave-ws-common-util):${CLASSPATH}

#
# Transform the classpath into a comma-separated list also
#
LIBJARS=`echo $CLASSPATH | sed 's/:/,/g'`

SCHEDULER_OPTIONS="-D mapreduce.job.queuename=bulkIngestQueue"

export HADOOP_CLASSPATH=${CLASSPATH}

RETURN_CODE_1=255
if ((`pgrep -f "\-Dapp=IngestMetricsSummaryLoader" | wc -l`==0))
then
	$MAP_FILE_LOADER_COMMAND_PREFIX $INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_METRICS_CORE_JAR} datawave.metrics.analytic.IngestMetricsSummaryLoader -Dapp=IngestMetricsSummaryLoader $SCHEDULER_OPTIONS \
	-libjars $LIBJARS -instance $INGEST_INSTANCE_NAME -zookeepers $INGEST_ZOOKEEPERS -user $USERNAME -password $PASSWORD $@
	RETURN_CODE_1=$?
fi

RETURN_CODE_2=255

RETURN_CODE_3=255
if ((`pgrep -f "\-Dapp=QueryMetricsSummaryLoader" | wc -l`==0))
then
	$MAP_FILE_LOADER_COMMAND_PREFIX $INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_METRICS_CORE_JAR} datawave.metrics.analytic.QueryMetricsSummaryLoader -Dapp=QueryMetricsSummaryLoader $SCHEDULER_OPTIONS \
	-libjars $LIBJARS -instance $INGEST_INSTANCE_NAME -zookeepers $INGEST_ZOOKEEPERS -user $USERNAME -password $PASSWORD $@
	RETURN_CODE_3=$?
fi

RETURN_CODE_4=255
if ((`pgrep -f "\-Dapp=FileByteSummaryLoader" | wc -l`==0))
then
	$MAP_FILE_LOADER_COMMAND_PREFIX $INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_METRICS_CORE_JAR} datawave.metrics.analytic.FileByteSummaryLoader -Dapp=FileByteSummaryLoader $SCHEDULER_OPTIONS \
	-libjars $LIBJARS -instance $INGEST_INSTANCE_NAME -zookeepers $INGEST_ZOOKEEPERS -user $USERNAME -password $PASSWORD $@
	RETURN_CODE_4=$?
fi

RETURN_CODE_5=255
if ((`pgrep -f "\-Dapp=HourlyQueryMetricsSummaryLoader" | wc -l`==0))
then
	$MAP_FILE_LOADER_COMMAND_PREFIX $INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_METRICS_CORE_JAR} datawave.metrics.analytic.QueryMetricsSummaryLoader -Dapp=HourlyQueryMetricsSummaryLoader $SCHEDULER_OPTIONS \
	-D metrics.use.hourly.precision=true \
	-libjars $LIBJARS -instance $INGEST_INSTANCE_NAME -zookeepers $INGEST_ZOOKEEPERS -user $USERNAME -password $PASSWORD $@
	RETURN_CODE_5=$?
fi

RETURN_CODE_6=255
if ((`pgrep -f "\-Dapp=HourlyIngestMetricsSummaryLoader" | wc -l`==0))
then
	$MAP_FILE_LOADER_COMMAND_PREFIX $INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_METRICS_CORE_JAR} datawave.metrics.analytic.IngestMetricsSummaryLoader -Dapp=HourlyIngestMetricsSummaryLoader $SCHEDULER_OPTIONS \
	-D metrics.use.hourly.precision=true \
	-libjars $LIBJARS -instance $INGEST_INSTANCE_NAME -zookeepers $INGEST_ZOOKEEPERS -user $USERNAME -password $PASSWORD $@
	RETURN_CODE_6=$?
fi

RETURN_CODE=$((RETURN_CODE_1 || RETURN_CODE_2 || RETURN_CODE_3 || RETURN_CODE_4 || RETURN_CODE_5 || RETURN_CODE_6))

exit $RETURN_CODE

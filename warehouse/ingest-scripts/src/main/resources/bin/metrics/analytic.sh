#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
  THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
  THIS_SCRIPT=`readlink -f $0`
fi

if ((`pgrep -f "\-Dapp=MetricsCorrelator" | wc -l`==0))
then

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

	CLASSPATH=${DATAWAVE_METRICS_CORE_JAR}:${CLASSPATH}
	CLASSPATH=${DATAWAVE_INGEST_CORE_JAR}:${CLASSPATH}

	CLASSPATH=$(findJar gson):${CLASSPATH}
	CLASSPATH=$(findJar libthrift):${CLASSPATH}
	CLASSPATH=$(findJar guava):${CLASSPATH}
	CLASSPATH=$(findJar javatuples):${CLASSPATH}
	CLASSPATH=$(findJar datawave-core):${CLASSPATH}
	CLASSPATH=$(findJar common-utils):${CLASSPATH}

	#
	# Transform the classpath into a comma-separated list also
	#
	LIBJARS=`echo $CLASSPATH | sed 's/:/,/g'`


	export HADOOP_CLASSPATH=${CLASSPATH}
	$MAP_FILE_LOADER_COMMAND_PREFIX $INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_METRICS_CORE_JAR} datawave.metrics.analytic.MetricsCorrelator -Dapp=MetricsCorrelator -D mapreduce.job.queuename=bulkIngestQueue -libjars $LIBJARS -instance $INGEST_INSTANCE_NAME -zookeepers $INGEST_ZOOKEEPERS -user $USERNAME -password $PASSWORD  $@
	RETURN_CODE=$?

else
        echo `date` "MetricsCorrelator already being ingested" >> $LOG_DIR/MetricsCorrelator.log
	RETURN_CODE=255
fi

exit $RETURN_CODE


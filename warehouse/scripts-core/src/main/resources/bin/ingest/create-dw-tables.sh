#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
	THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
	THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

#
# Ensure force is set so we can run this without ingest running
#
if [[ ! "$@" =~ ".*-force.*" && ! "$@" =~ "-force" ]]; then
  $THIS_SCRIPT -force
  exit $?
fi

#
# Get the ingest environment
#
. ./ingest-env.sh

#
# Get the classpath
#
. ./ingest-libs.sh

if [[ -z $DATAWAVE_INGEST_HOME ]]; then
  export DATAWAVE_INGEST_HOME=$THIS_DIR/../..
fi

#
# Capture only the ingest config files
#
declare -a INGEST_CONFIG
i=0
for f in ../../config/*-config.xml; do
  INGEST_CONFIG[i++]=`basename $f`
done

#
# Transform the classpath into a comma-separated list also
#
LIBJARS=`echo $CLASSPATH | sed 's/:/,/g'`

#
# The '-createTablesOnly' flag requires no input directory but the code still requires something to be provided
#
INPUT_FILES=/dev/null

export HADOOP_CLASSPATH=$CLASSPATH
export HADOOP_OPTS="-Dfile.encoding=UTF8 -Duser.timezone=GMT $HADOOP_INGEST_OPTS"

echo "$INGEST_HADOOP_HOME/bin/hadoop jar ../../lib/datawave-ingest-core-$INGEST_VERSION.jar nsa.datawave.ingest.mapreduce.job.IngestJob -jt $INGEST_JOBTRACKER_NODE $INPUT_FILES ${INGEST_CONFIG[@]} -cacheBaseDir $JOB_CACHE_DIR -cacheJars $LIBJARS -user $USERNAME -pass $PASSWORD -instance $WAREHOUSE_INSTANCE_NAME -zookeepers $WAREHOUSE_ZOOKEEPERS -createTablesOnly $EXTRA_OPTS"
$INGEST_HADOOP_HOME/bin/hadoop jar ../../lib/datawave-ingest-core-$INGEST_VERSION.jar nsa.datawave.ingest.mapreduce.job.IngestJob -jt $INGEST_JOBTRACKER_NODE $INPUT_FILES ${INGEST_CONFIG[@]} -cacheBaseDir $JOB_CACHE_DIR -cacheJars $LIBJARS -user $USERNAME -pass $PASSWORD -instance $WAREHOUSE_INSTANCE_NAME -zookeepers $WAREHOUSE_ZOOKEEPERS -createTablesOnly $EXTRA_OPTS

RETURN_CODE=$?

exit $RETURN_CODE

#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

#
# Ensure force is set so we can run this without ingest running
#
if [[ ! "$@" =~ .*-force.* && ! "$@" =~ "-force" ]]; then
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
  INGEST_CONFIG[i++]=$(basename $f)
done

export HADOOP_CLASSPATH=$CLASSPATH
export HADOOP_OPTS="-Dfile.encoding=UTF8 -Duser.timezone=GMT $HADOOP_INGEST_OPTS"

echo "$INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_INGEST_CORE_JAR} datawave.ingest.TableCreator -user $USERNAME -pass $PASSWORD -instance $WAREHOUSE_INSTANCE_NAME -zookeepers $WAREHOUSE_ZOOKEEPERS ${INGEST_CONFIG[@]} $EXTRA_OPTS"
$INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_INGEST_CORE_JAR} datawave.ingest.TableCreator -user $USERNAME -pass $PASSWORD -instance $WAREHOUSE_INSTANCE_NAME -zookeepers $WAREHOUSE_ZOOKEEPERS ${INGEST_CONFIG[@]} $EXTRA_OPTS

RETURN_CODE=$?

exit $RETURN_CODE

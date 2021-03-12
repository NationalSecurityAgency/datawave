#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
	READLINK_CMD="python -c 'import os,sys;print os.path.realpath(sys.argv[1])'"
else
	READLINK_CMD="readlink -f"
fi
THIS_SCRIPT=`eval $READLINK_CMD $0`
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

. ../ingest/ingest-env.sh

#read from the datawave metadata table to create the edge key version file and save it locally with the rest of the config files
./create-edgekey-version-cache.sh --update ../../config

. ../ingest/ingest-libs.sh

JOB_CACHE_CLASSPATH="--classpath ${CLASSPATH}"
CLASSPATH=$CLASSPATH:`$HADOOP_HOME/bin/hadoop classpath`
HADOOP_NATIVE_LIB_DIR="$INGEST_HADOOP_HOME/lib/native"
JAVA_OPTS="${JAVA_OPTS} -Djava.library.path=${HADOOP_NATIVE_LIB_DIR}"

export HADOOP_CLASSPATH=$CLASSPATH
export INGEST_HADOOP_CONF
export WAREHOUSE_HADOOP_CONF

$JAVA_HOME/bin/java $JAVA_OPTS -cp $CLASSPATH datawave.ingest.util.cache.load.LoadJobCacheLauncher ${JOB_CACHE_CLASSPATH} "$@" 2>&1 < /dev/null &


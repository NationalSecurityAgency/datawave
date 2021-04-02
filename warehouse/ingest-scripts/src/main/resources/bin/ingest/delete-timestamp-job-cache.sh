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
. ../ingest/ingest-libs.sh

CLASSPATH=$CLASSPATH:`$HADOOP_HOME/bin/hadoop classpath`
HADOOP_NATIVE_LIB_DIR="$INGEST_HADOOP_HOME/lib/native"
JAVA_OPTS="${JAVA_OPTS} -Djava.library.path=${HADOOP_NATIVE_LIB_DIR}"

export HADOOP_CLASSPATH=$CLASSPATH
export INGEST_HADOOP_CONF
export WAREHOUSE_HADOOP_CONF

$JAVA_HOME/bin/java $JAVA_OPTS -cp $CLASSPATH datawave.ingest.util.cache.delete.DeleteJobCacheLauncher "$@" 2>&1 < /dev/null

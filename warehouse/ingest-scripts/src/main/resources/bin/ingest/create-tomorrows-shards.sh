#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

. ./ingest-libs.sh

declare -i numshards=$NUM_SHARDS
if [[ "$1" == "" ]]; then
  declare -i numerator=1
else
  declare -i numerator=$1
fi
if [[ "$2" == "" ]]; then
  declare -i divisor=$numerator
  numerator=1
else
  declare -i divisor=$2
fi
numshards=$((numshards * numerator / divisor))

shardsPerSplit=1
if [[ -n "$3" ]]; then
    shardsPerSplit=$3
fi

DATE=`date -d tomorrow +%Y%m%d`

echo "Generating ${numerator}/${divisor} of ${NUM_SHARDS} = $numshards shards for $DATE"

TYPES=${BULK_INGEST_DATA_TYPES},${LIVE_INGEST_DATA_TYPES},${COMPOSITE_DATA_TYPES}

ADDJARS=$THIS_DIR/$DATAWAVE_INGEST_CORE_JAR:$THIS_DIR/$COMMON_UTIL_JAR:$THIS_DIR/$DATAWAVE_CORE_JAR:$THIS_DIR/$DATAWAVE_COMMON_UTILS_JAR:$THIS_DIR/$COMMONS_LANG_JAR

CLASSPATH=$ADDJARS $WAREHOUSE_ACCUMULO_HOME/bin/accumulo datawave.ingest.util.GenerateShardSplits $DATE 1 ${numshards} ${shardsPerSplit} -addShardMarkers -addDataTypeMarkers $TYPES $USERNAME $PASSWORD ${SHARD_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

CLASSPATH=$ADDJARS $WAREHOUSE_ACCUMULO_HOME/bin/accumulo datawave.ingest.util.GenerateShardSplits $DATE 1 ${numshards} ${shardsPerSplit} -addShardMarkers -addDataTypeMarkers $TYPES $USERNAME $PASSWORD ${ERROR_SHARD_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

CLASSPATH=$ADDJARS $WAREHOUSE_ACCUMULO_HOME/bin/accumulo datawave.ingest.util.GenerateShardSplits $DATE 1 ${numshards} ${shardsPerSplit} -addShardMarkers $USERNAME $PASSWORD ${QUERYMETRICS_SHARD_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

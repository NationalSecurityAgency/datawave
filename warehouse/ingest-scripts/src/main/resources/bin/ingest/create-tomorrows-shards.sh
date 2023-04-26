#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

. ./ingest-libs.sh

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

shardsPerSplit=1
if [[ -n "$3" ]]; then
    shardsPerSplit=$3
fi

if [[ "$4" == "" ]]; then
  declare -i shardTableShards=$NUM_SHARDS
else
  declare -i shardTableShards=$4
fi

if [[ "$5" == "" ]]; then
  declare -i errorTableShards=$NUM_SHARDS
else
  declare -i errorTableShards=$5
fi

if [[ "$6" == "" ]]; then
  declare -i queryMetricShards=$NUM_SHARDS
else
  declare -i queryMetricShards=$6
fi

shardTableNumShards=$((shardTableShards * numerator / divisor))
errorTableNumShards=$((errorTableShards * numerator / divisor))
queryMetricTableNumShards=$((queryMetricShards * numerator / divisor))

DATE=`date -d tomorrow +%Y%m%d`

echo "Generating ${numerator}/${divisor} of ${shardTableShards} = $shardTableNumShards shards for $DATE"
echo "Generating ${numerator}/${divisor} of ${errorTableShards} = $errorTableNumShards shards for $DATE"
echo "Generating ${numerator}/${divisor} of ${queryMetricShards} = $queryMetricTableNumShards shards for $DATE"

TYPES=${BULK_INGEST_DATA_TYPES},${LIVE_INGEST_DATA_TYPES},${COMPOSITE_DATA_TYPES}

ADDJARS=$THIS_DIR/$DATAWAVE_INGEST_CORE_JAR:$THIS_DIR/$COMMON_UTIL_JAR:$THIS_DIR/$DATAWAVE_CORE_JAR:$THIS_DIR/$DATAWAVE_COMMON_UTILS_JAR:$THIS_DIR/$COMMONS_LANG_JAR

CLASSPATH=$ADDJARS $WAREHOUSE_ACCUMULO_HOME/bin/accumulo datawave.ingest.util.GenerateShardSplits $DATE 1 ${shardTableNumShards} ${shardsPerSplit} -addShardMarkers -addDataTypeMarkers $TYPES $USERNAME $PASSWORD ${SHARD_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

CLASSPATH=$ADDJARS $WAREHOUSE_ACCUMULO_HOME/bin/accumulo datawave.ingest.util.GenerateShardSplits $DATE 1 ${errorTableNumShards} ${shardsPerSplit} -addShardMarkers -addDataTypeMarkers $TYPES $USERNAME $PASSWORD ${ERROR_SHARD_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

CLASSPATH=$ADDJARS $WAREHOUSE_ACCUMULO_HOME/bin/accumulo datawave.ingest.util.GenerateShardSplits $DATE 1 ${queryMetricTableNumShards} ${shardsPerSplit} -addShardMarkers $USERNAME $PASSWORD ${QUERYMETRICS_SHARD_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

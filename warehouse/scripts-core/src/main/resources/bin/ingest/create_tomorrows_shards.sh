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

DATE=`date -d tomorrow +%Y%m%d`

echo "Generating ${numerator}/${divisor} of ${NUM_SHARDS} = $numshards shards for $DATE"

TYPES=${ONE_HR_INGEST_DATA_TYPES},${FIFTEEN_MIN_INGEST_DATA_TYPES},${FIVE_MIN_INGEST_DATA_TYPES},${BULK_INGEST_DATA_TYPES},${LIVE_INGEST_DATA_TYPES},${COMPOSITE_DATA_TYPES}

ADDJARS=$THIS_DIR/$DATAWAVE_INGEST_CORE_JAR,$THIS_DIR/$COMMON_UTIL_JAR,$THIS_DIR/$JODA_TIME_JAR,$THIS_DIR/$DATAWAVE_CORE_JAR

$WAREHOUSE_ACCUMULO_HOME/bin/accumulo -add $ADDJARS nsa.datawave.ingest.util.GenerateShardSplits $DATE 1 ${numshards} -addShardMarkers -addDataTypeMarkers $TYPES $USERNAME $PASSWORD ${SHARD_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

$WAREHOUSE_ACCUMULO_HOME/bin/accumulo -add $ADDJARS nsa.datawave.ingest.util.GenerateShardSplits $DATE 1 ${numshards} -addShardMarkers -addDataTypeMarkers $TYPES $USERNAME $PASSWORD ${KNOWLEDGE_SHARD_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

$WAREHOUSE_ACCUMULO_HOME/bin/accumulo -add $ADDJARS nsa.datawave.ingest.util.GenerateShardSplits $DATE 1 ${numshards} -addShardMarkers -addDataTypeMarkers $TYPES $USERNAME $PASSWORD ${ERROR_SHARD_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

$WAREHOUSE_ACCUMULO_HOME/bin/accumulo -add $ADDJARS nsa.datawave.ingest.util.GenerateShardSplits $DATE 1 ${numshards} -addShardMarkers $USERNAME $PASSWORD ${QUERY_METRICS_BASE_NAME}_e $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

#!/bin/bash
# Usage: create_tomorrows_shards.sh (numerator) (divisor) (errorTableShards) (queryMetricShards)
# $1 - numerator for the fraction of shards
# $2 - divisor for the fraction of shards
# $3 - total number of shards for the error shard table
# $4 - total number of shards for the query metrics table
# The script will create a new set of shards for the next day, based on the fraction of numerator/divisor as well as the total number of shards defined per table.
# ex:
# create_tomorrows_shards.sh 1 1 250 100
# Generating 1/1 of 311 = 311 shards for 20230809
# Generating 1/1 of 250 = 250 error table shards for 20230809
# Generating 1/1 of 100 = 100 query metric shards for 20230809

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

. ./ingest-libs.sh

declare -i numshardsOrg=$NUM_SHARDS

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

if [[ "$3" == "" ]]; then
  declare -i errorTableShards=$numshardsOrg
else
  declare -i errorTableShards=$3
fi

if [[ "$4" == "" ]]; then
  declare -i queryMetricShards=$numshardsOrg
else
  declare -i queryMetricShards=$4
fi

shardTableNumShards=$((numshardsOrg * numerator / divisor))
errorTableNumShards=$((errorTableShards * numerator / divisor))
queryMetricTableNumShards=$((queryMetricShards * numerator / divisor))

DATE=$(date -d tomorrow +%Y%m%d)

echo "Generating ${numerator}/${divisor} of ${numshardsOrg} = $shardTableNumShards shards for $DATE"
echo "Generating ${numerator}/${divisor} of ${errorTableShards} = $errorTableNumShards shards for $DATE"
echo "Generating ${numerator}/${divisor} of ${queryMetricShards} = $queryMetricTableNumShards shards for $DATE"

TYPES=${BULK_INGEST_DATA_TYPES},${LIVE_INGEST_DATA_TYPES},${COMPOSITE_DATA_TYPES}

ADDJARS=$THIS_DIR/$DATAWAVE_INGEST_CORE_JAR:$THIS_DIR/$COMMON_UTIL_JAR:$THIS_DIR/$DATAWAVE_CORE_JAR:$THIS_DIR/$DATAWAVE_COMMON_UTILS_JAR:$THIS_DIR/$COMMONS_LANG_JAR

daysToGenerate=1

CLASSPATH=$ADDJARS $WAREHOUSE_ACCUMULO_HOME/bin/accumulo datawave.ingest.util.GenerateShardSplits $DATE ${daysToGenerate} ${shardTableNumShards} ${SHARD_TABLE_SHARDNUM_SPLIT_STEP} -addShardMarkers -addDataTypeMarkers $TYPES $USERNAME $PASSWORD ${SHARD_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

CLASSPATH=$ADDJARS $WAREHOUSE_ACCUMULO_HOME/bin/accumulo datawave.ingest.util.GenerateShardSplits $DATE ${daysToGenerate} ${errorTableNumShards} ${ERROR_SHARD_TABLE_SHARDNUM_SPLIT_STEP} -addShardMarkers -addDataTypeMarkers $TYPES $USERNAME $PASSWORD ${ERROR_SHARD_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

CLASSPATH=$ADDJARS $WAREHOUSE_ACCUMULO_HOME/bin/accumulo datawave.ingest.util.GenerateShardSplits $DATE ${daysToGenerate} ${queryMetricTableNumShards} ${QUERY_METRICS_SHARD_TABLE_SHARDNUM_SPLIT_STEP} -addShardMarkers $USERNAME $PASSWORD ${QUERYMETRICS_SHARD_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

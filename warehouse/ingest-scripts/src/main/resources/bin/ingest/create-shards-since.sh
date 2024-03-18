#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

. ./ingest-libs.sh

# The date must be in yyyymmdd format
if [[ "$1" == "" ]]; then
    echo "Please specify a date (yyyyMMdd) for which to process data"
    exit 1
fi

date=$(date --date="$1" +%Y%m%d)
if [[ "$?" != "0" ]]; then
    echo " Please specify a valid date (yyyyMMdd) for which to process data"
    exit 1
fi

date2stamp (){
    date --utc --date "$1" +%s
}

dateDiff (){
    declare -i dte1=$(date2stamp $1)
    declare -i dte2=$(date2stamp $2)
    declare -i diffSec=$((dte2-dte1))
    declare -i sec=86400
    if (( diffSec < sec )); then
        diffSec=0
    fi
    echo $((1 + diffSec/sec))
}

if [[ "$2" == "" ]]; then
    # set the count to the number of days up until today
    end=$(date +%Y%m%d)
    count=$(dateDiff $date $end)
else
    count=$2
fi

shardsPerSplit=1
if [[ -n "$3" ]]; then
    shardsPerSplit=$3
fi

TABLES="${SHARD_TABLE_NAME},${ERROR_SHARD_TABLE_NAME},${QUERYMETRICS_SHARD_TABLE_NAME}"
if [[ -n "$4" ]]; then
    # user-specified set of tables to generate shard splits on
    # must be comma-delimited, no spaces
    TABLES="$4"
fi

OLD_IFS="$IFS"
IFS=","
TABLES=( ${TABLES} )
IFS="$OLD_IFS"

TYPES=${BULK_INGEST_DATA_TYPES},${LIVE_INGEST_DATA_TYPES},${COMPOSITE_DATA_TYPES}

ADDJARS=$THIS_DIR/$DATAWAVE_INGEST_CORE_JAR:$THIS_DIR/$COMMON_UTIL_JAR:$THIS_DIR/$DATAWAVE_CORE_JAR:$THIS_DIR/$DATAWAVE_COMMON_UTILS_JAR:$THIS_DIR/$COMMONS_LANG_JAR

for table in "${TABLES[@]}" ; do
   CLASSPATH=$ADDJARS $WAREHOUSE_ACCUMULO_BIN/accumulo datawave.ingest.util.GenerateShardSplits $date $count ${NUM_SHARDS} $shardsPerSplit -addShardMarkers $USERNAME $PASSWORD ${table} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS
done

#!/bin/bash
#
# rfile-split-coverage.sh <inputDir> <tableName> <outputDir>
#
# Runs a MapReduce job that analyzes RFiles in a bulk import directory to determine
# how each RFile's key range maps to the current table splits in Accumulo.
#
# For each RFile, the job outputs the number and percentage of table splits that
# the file's row range covers.
#
# Arguments:
#   inputDir  - HDFS path to the directory containing .rf files
#   tableName - Accumulo table name to fetch current splits for
#   outputDir - HDFS output directory for results
#

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

#
# Get the classpath
#
. ../ingest/ingest-libs.sh

#
# Get the job cache directory
#
. ../ingest/job-cache-env.sh

if [[ -z $DATAWAVE_INGEST_HOME ]]; then
  export DATAWAVE_INGEST_HOME=$THIS_DIR/../..
fi

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <inputDir> <tableName> <outputDir>"
  echo ""
  echo "  inputDir  - HDFS path to the directory containing .rf files"
  echo "  tableName - Accumulo table name to fetch current splits for"
  echo "  outputDir - HDFS output directory for results"
  exit 1
fi

INPUT_DIR=$1
TABLE_NAME=$2
OUTPUT_DIR=$3

#
# Capture the ingest config files (these provide Accumulo connection info)
#
declare -a INGEST_CONFIG
i=0
for f in ../../config/*-config.xml; do
  INGEST_CONFIG[i++]=$(basename $f)
done

export HADOOP_CLASSPATH=$CLASSPATH
export HADOOP_OPTS="-Dfile.encoding=UTF8 -Duser.timezone=GMT $HADOOP_INGEST_OPTS"

CMD="$INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_INGEST_CORE_JAR} \
datawave.ingest.mapreduce.job.RFileSplitCoverageJob \
${INPUT_DIR} ${TABLE_NAME} ${OUTPUT_DIR} \
${INGEST_CONFIG[@]}"

echo $CMD
$CMD

RETURN_CODE=$?

exit $RETURN_CODE

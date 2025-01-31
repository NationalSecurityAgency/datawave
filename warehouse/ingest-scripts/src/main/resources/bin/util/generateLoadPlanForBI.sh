#!/bin/bash
# Exit the script if any command fails
set -e

if [[ `uname` == "Darwin" ]]; then
	THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
	THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

#
# Get the classpath
#
. ../ingest/ingest-libs.sh

#
# Get the job cache directory
#
. ../ingest/job-cache-env.sh

# Script Usage
if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <bulk load base directory>"
  exit 1
fi

# HDFS Input Directory (contains subdirectories)
INPUT_DIR=$1

CLASSPATH="${DATAWAVE_INGEST_CORE_JAR}:$CLASSPATH"
#
# Transform the classpath into a comma-separated list also
#
LIBJARS=`echo $CLASSPATH | sed 's/:/,/g'`

export HADOOP_CLASSPATH="${DATAWAVE_INGEST_CORE_JAR}:$CLASSPATH"

hadoop jar "${DATAWAVE_INGEST_CORE_JAR}" datawave.ingest.mapreduce.job.util.GenerateLoadPlanDriver \
 -biDir $INPUT_DIR -cacheBaseDir $JOB_CACHE_DIR -cacheJars $LIBJARS -splitsCacheDir /data/splitsCache

# Exit message
if [ $? -eq 0 ]; then
  echo "Load plan generation completed successfully!"
else
  echo "Load plan generation failed!"
fi
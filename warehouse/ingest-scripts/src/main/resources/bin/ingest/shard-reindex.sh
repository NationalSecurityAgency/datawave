#!/bin/bash

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
. ./ingest-libs.sh

#
# Get the job cache directory
#
. ./job-cache-env.sh

if [[ -z $DATAWAVE_INGEST_HOME ]]; then
  export DATAWAVE_INGEST_HOME=$THIS_DIR/../..
fi

#
# Capture only the ingest config files
#
declare -a INGEST_CONFIG
i=0
for f in ../../config/*-config.xml; do
  INGEST_CONFIG[i++]=`basename $f`
done

#
# Transform the classpath into a comma-separated list also
#
LIBJARS=`echo $CLASSPATH | sed 's/:/,/g'`

DATE=`date "+%Y%m%d%H%M%S"`
# specifying no partitioning argument will default to the MultiTableRangePartitioner
PART_ARG=
INPUT_FILES=$1
REDUCERS=$2
OUTPUT_DIR=$3
WORKDIR=${OUTPUT_DIR}/${DATE}-$$/
EXTRA_OPTS=${@:4}

export HADOOP_CLASSPATH=$CLASSPATH
export HADOOP_OPTS="-Dfile.encoding=UTF8 -Duser.timezone=GMT $HADOOP_INGEST_OPTS"

# update the config to be comma separated
RESOURCES=$(echo ${INGEST_CONFIG[@]} | tr ' ' ',')

CMD="$INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_INGEST_CORE_JAR} datawave.ingest.mapreduce.job.ShardReindexJob --cacheDir $JOB_CACHE_DIR --cacheJars $LIBJARS --username $USERNAME --password $PASSWORD --instance $WAREHOUSE_INSTANCE_NAME --zookeepers $WAREHOUSE_ZOOKEEPERS --reducers $REDUCERS --outputDir $OUTPUT_DIR --inputFiles $INPUT_FILES --workDir $WORKDIR --resources $RESOURCES  --sourceHdfs $INGEST_HDFS_NAME_NODE --destHdfs $INGEST_HDFS_NAME_NODE $EXTRA_OPTS"

echo $CMD
$CMD

RETURN_CODE=$?

exit $RETURN_CODE

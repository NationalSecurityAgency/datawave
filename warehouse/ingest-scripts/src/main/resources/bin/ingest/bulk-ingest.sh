#!/bin/bash

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
  INGEST_CONFIG[i++]=$(basename $f)
done

#
# Transform the classpath into a comma-separated list also
#
LIBJARS=$(echo $CLASSPATH | sed 's/:/,/g')

#
# Ingest parameters
#
if [[ "$BULK_CHILD_MAP_MAX_MEMORY_MB" == "" ]]; then
   BULK_CHILD_MAP_MAX_MEMORY_MB=2536
fi
if [[ "$BULK_CHILD_REDUCE_MAX_MEMORY_MB" == "" ]]; then
   BULK_CHILD_REDUCE_MAX_MEMORY_MB=2536
fi
# Tell Yarn that we're using 20% more memory than we've requested for the VM
# to account for off heap memory usage.
MAP_MEMORY_MB=$(( ((BULK_CHILD_MAP_MAX_MEMORY_MB*1048576*12)/10)/1048576  ))
REDUCE_MEMORY_MB=$(( ((BULK_CHILD_REDUCE_MAX_MEMORY_MB*1048576*12)/10)/1048576  ))

DATE=$(date "+%Y%m%d%H%M%S")
WORKDIR=${BASE_WORK_DIR}/${DATE}-$$/
# specifying no partitioning argument will default to the MultiTableRangePartitioner
PART_ARG=
TIMEOUT=600000
INPUT_FILES=$1
REDUCERS=$2
EXTRA_OPTS=${@:3}
# Customize accumulo client properties for AccumuloOutputFormat (newline-separated, one property per line)
BATCHWRITER_OPTS="-AccumuloOutputFormat.ClientOpts.ClientProps=
batch.writer.memory.max=100000000B
batch.writer.threads.max=4
"
MAPRED_OPTS="-mapreduce.map.memory.mb=$MAP_MEMORY_MB -mapreduce.reduce.memory.mb=$REDUCE_MEMORY_MB -mapreduce.job.reduces=$REDUCERS -mapreduce.task.io.sort.mb=${BULK_CHILD_IO_SORT_MB} -mapreduce.task.io.sort.factor=100 -bulk.ingest.mapper.threads=0 -bulk.ingest.mapper.workqueue.size=10000 -io.file.buffer.size=1048576 -dfs.bytes-per-checksum=4096 -io.sort.record.percent=.10 -mapreduce.map.output.compress=${BULK_MAP_OUTPUT_COMPRESS} -mapreduce.map.output.compress.codec=${BULK_MAP_OUTPUT_COMPRESSION_CODEC} -mapreduce.output.fileoutputformat.compress.type=${BULK_MAP_OUTPUT_COMPRESSION_TYPE} $PART_ARG -mapreduce.task.timeout=$TIMEOUT -markerFileReducePercentage 0.33 -context.writer.max.cache.size=2500 -mapreduce.job.queuename=bulkIngestQueue $MAPRED_INGEST_OPTS"
#-mapreduce.map.sort.spill.percent=.50

export HADOOP_CLASSPATH=$CLASSPATH
export HADOOP_OPTS="-Dfile.encoding=UTF8 -Duser.timezone=GMT $HADOOP_INGEST_OPTS"
#
export CHILD_MAP_OPTS="-Xmx${BULK_CHILD_MAP_MAX_MEMORY_MB}m -XX:+UseConcMarkSweepGC -Dfile.encoding=UTF8 -Duser.timezone=GMT -XX:+UseNUMA $CHILD_INGEST_OPTS"
export CHILD_REDUCE_OPTS="-Xmx${BULK_CHILD_REDUCE_MAX_MEMORY_MB}m -XX:+UseConcMarkSweepGC -Dfile.encoding=UTF8 -Duser.timezone=GMT -XX:+UseNUMA $CHILD_INGEST_OPTS"

echo $INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_INGEST_CORE_JAR} datawave.ingest.mapreduce.job.IngestJob -jt $INGEST_JOBTRACKER_NODE $INPUT_FILES ${INGEST_CONFIG[@]} -cacheBaseDir $JOB_CACHE_DIR -cacheJars $LIBJARS -user $USERNAME -pass $PASSWORD -instance $WAREHOUSE_INSTANCE_NAME -zookeepers $WAREHOUSE_ZOOKEEPERS -workDir $WORKDIR  -flagFileDir ${FLAG_DIR} -flagFilePattern '.*_(bulk)_.*\.flag' -srcHdfs $INGEST_HDFS_NAME_NODE -destHdfs $WAREHOUSE_HDFS_NAME_NODE -distCpConfDir $WAREHOUSE_HADOOP_CONF -mapreduce.map.java.opts=\"$CHILD_MAP_OPTS\" -mapreduce.reduce.java.opts=\"$CHILD_REDUCE_OPTS\" "${BATCHWRITER_OPTS}" $MAPRED_OPTS $EXTRA_OPTS

$INGEST_HADOOP_HOME/bin/hadoop jar ${DATAWAVE_INGEST_CORE_JAR} datawave.ingest.mapreduce.job.IngestJob -jt $INGEST_JOBTRACKER_NODE $INPUT_FILES ${INGEST_CONFIG[@]} -cacheBaseDir $JOB_CACHE_DIR -cacheJars $LIBJARS -user $USERNAME -pass $PASSWORD -instance $WAREHOUSE_INSTANCE_NAME -zookeepers $WAREHOUSE_ZOOKEEPERS -workDir $WORKDIR  -flagFileDir ${FLAG_DIR} -flagFilePattern '.*_(bulk)_.*\.flag' -srcHdfs $INGEST_HDFS_NAME_NODE -destHdfs $WAREHOUSE_HDFS_NAME_NODE -distCpConfDir $WAREHOUSE_HADOOP_CONF -mapreduce.map.java.opts="$CHILD_MAP_OPTS" -mapreduce.reduce.java.opts="$CHILD_REDUCE_OPTS" "${BATCHWRITER_OPTS}" $MAPRED_OPTS $EXTRA_OPTS

RETURN_CODE=$?

exit $RETURN_CODE


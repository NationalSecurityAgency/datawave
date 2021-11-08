#!/bin/bash

#========================================================================================
# Purpose
# Creates index statistics for a single date from the data in the shard table. The input
# argument should be a single day, however, for testing and development purposes the
# number of shards can be limited.
#
# USAGE:
#    SCRIPT [date] [number of shards]
#    SCRIPT CSV list of shards
# examples
#    SCRIPT  => generate data for shards for the current day
#    SCRIPT 20180909 => generate data for shards 20180909_0 ... _n (where n = $NUM_SHARDS)
#    SCRIPT 20180909 3 => generate data for shards 20180909_0, ... _2 (shards 0 1 2)
#    SCRIPT 20180909_1 => generate data for shard 20180909_1
#    SCRIPT 20180909_1,20180909_3 => generate data for shard 20180909_1 and 20180909_3
#
#++++++++++++++++++++++++++++++++++++++++++++++++
# HyperLogLogPlus
# The HyperlogLogPlus algorithm is utilized to provide an estimate of the cardinality of
# a dataset. There are two parameters that affect the memory and precision of the
# algorithm, a sparse and normal precision. These parameters determine how many buckets
# are allocated to represent the data, which corresponds to the amount of memory
# utilization. Valid values are between 4 and 32 inclusive and the normal precision
# cannot be greater than the sparse precision. The sparse representation is used for
# smaller data sets and is automatically converted to the normal precision when the
# data set exceeds a threshold based upon the sparse precision. The sparse representation
# can be overriden by setting the value to 0. This will result in the use of the normal
# representation only. Setting the sparse precision to 0 has not been tested.
#
# Higher precision values translate to more buckets and thus higher memory utilization.
# The number of buckets that are allocated is modeled by  the equation:
#
#        buckets = 2 ** n  => where n is the precision
#
# As the precision is increased, the error percentage decreases and is modeled by
# the equation:
#
#        error percentage = 1.0 / SQRT(m)   => where m is the number of buckets
#
# Precision values should be set based upon memory usage versus error percentage. Several
# tests indicate that inconsistent results can occur when the sparse and normal precision
# values differ. The greater the difference, the larger the error percentage. While most
# of the generated cardinalities fell within the projected error percentage, in the few
# instances where it did it was off by a very significant factor. Thus when selecting
# sparse and normal precision values it is best to choose the same value for both.
#
#+++++++++++++++++++++++++++++++++++++++
# Optional Environmental Variables
# _MapperMemoryMB => memory allocated in MB to each mapper JVM process
# _ReducerMemoryMB => memory allocated in MB to each reducer JVM process
#
# Optional Debug Environment Variables
# There are several options that will allow for analysis of the debug data:
#   _UniqueCounts => Collects actual unique counts in addition to the HyperLogLogPlus
#           generated cardinality for analysis purposes. The mapper will generate the
#           the unique count, while the reducer is the best location to view all of the
#           entries from multiple mappers. The test script hlParse.sh is provided to
#           parse the log file of the reducer. This process extracts the analysis data
#           into a csv format, which can be loaded into a spreadsheet for analysis.
#           NOTE: The _MapperMemoryMB may need to be set to avoid an OOM condition.
#   _GCMapperLog => Enables GC logging for the mapper.
#   _GCReducerLog => Enables GC logging for the reducer.
#   _GCLog => Enables GC logging for both the mapper and reducer.
#   _GCLogDir => Specifies the directory for the GC logs (overrides the default value).
#   _HeapDump => Enable heap dump for mapper and reducer.
#========================================================================================

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

. ../ingest/ingest-env.sh

declare -r _base=$(basename $0)
declare -r _lockFile=${LOCK_FILE_DIR}/${_base}


#===================================================================================
# local functions
function cleanup() {
    declare -r _status=$1

    rm -f "${_lockFile}"
    trap 0 1 2 3 15
    exit ${_status}
}

function setupLockFile() {
    # check for current execution of this script
    test -e ${_lockFile} && {
        echo "ERROR: lock file exists => ${_lockFile}"
        exit 1
    }
    touch "${_lockFile}"
    trap "cleanup 1" 1 2 3 15
    trap "cleanup 0" 0
}

#==================================
# Sets JVM parameters for Mapper
function setMapperJVMParameters() {
    _MapperMemoryMB=${_MapperMemoryMB:-1024}
    # memory setting may need to be adjusted based upon input data
    # when _CountsOnly debug variable is set; check max container size

    # allocate 1/3 memory to mapper io sort buffer but must be less than 2048
    ((_MapInMemSortBufferMBSize = _MapperMemoryMB / 3))
    if [[ 1536 -le ${_MapInMemSortBufferMBSize} ]]; then
        _MapInMemSortBufferMBSize=1536
    fi

    # enable GC logging if requested - typically for debug purposes
    # NOTE: there should only be one mapper; same file is used for all mappers
    if [[ -n "${_GCLog}" || -n "${_GCMapperLog}" ]]; then
        local -r _logFile="-Xloggc:${_GCLogDir}/gcMap-$$-${_dateStr}.log"
        local -r _gcLog="${_GCLogOpts} ${_logFile}"
    fi

    _ChildMapOpts="\
-Xmx${_MapperMemoryMB}m \
${_DefaultJVMArgs} \
${CHILD_INGEST_OPTS} \
${_HeapDumpOpts} \
${_gcLog}"
}

function setReducerJVMParameters() {
    _ReducerMemoryMB=${_ReducerMemoryMB:-1024}

    # enable GC logging if requested - typically for debug purposes
    if [[ -n "${_GCLog}" || -n "${_GCMapperLog}" ]]; then
        local -r _logFile="-Xloggc:${_GCLogDir}/gcReduce-$$-${_dateStr}.log"
        local -r _gcLog="${_GCLogOpts} ${_logFile}"
    fi

    _ChildReduceOpts="\
-Xmx${_ReducerMemoryMB}m \
${_DefaultJVMArgs} \
${CHILD_INGEST_OPTS} \
${_HeapDumpOpts} \
${_gcLog}"
}

#==================================
# Sets required and optional parameters for the map/reduce job.
function setStatsParameters() {
    # default values for optional debug parameters
    # see class files for description
    #  =======  setting log level for mapper/redcuer/job
    #  -stats.job.log.level=info
    #  -stats.mapper.log.level=info
    #  -stats.reducer.log.level=info
    #  ======  settings for heartbeat intervals
    #  -stats.mapper.input.interval=10000000
    #  -stats.mapper.value.interval=100
    #  -stats.mapper.uniquecount=false
    #  -stats.reducer.value.interval=20
    #  -stats.reducer.counts=false
    local _StatsDebugOpts=""

    # check for unique counts
    test -n "${_UniqueCounts}" && {
        _StatsDebugOpts="${_StatsDebugOpts} -stats.mapper.uniquecount=true -stats.reducer.counts=true"
    }

    # runtime parameters are set in shard-stats-config.xml

    test -n "${_NumShards}" && {
        local -r _shardOpts="-num.shards=${_NumShards}"
    }

    # input and output table names
    _StatsOpts="
${_StatsDebugOpts} \
${_shardOpts} \
"
}

#==================================
# Determines the number of reducers to run based upon the number of shards.
function calculateReducers() {
    local -r _reducers=$1

    # divisor for number of reducers based upon the number of shards
    local -ir _divisor=6
    local -i _num
    if [[ -n "${_NumShards}" ]]; then
        ((_num = _NumShards / _divisor))
    else
        IFS=","
        set -- ${_argDate}
        unset IFS
        if [[ $# -gt 1 ]]; then
            ((_num = _cnt / _divisor))
        else
            ((_num = NUM_SHARDS / _divisor))
        fi
    fi

    test "${_num}" -eq 0 && _num=1

    eval ${_reducers}=${_num}
}

# end local functions
#===================================================================================

setupLockFile


#
# set the job cache
#
. ../ingest/job-cache-env.sh

#
# set the classpath
#
. ../ingest/ingest-libs.sh

if [[ -z "$DATAWAVE_INGEST_HOME" ]]; then
    export DATAWAVE_INGEST_HOME=$THIS_DIR/../..
fi

#
# set the ingest config files
#
declare -a _IngestConfig
i=0
for f in ../../config/shard-stats-config.xml ../../config/shard-ingest-config.xml; do
    _IngestConfig[i++]=$(basename $f)
done

#
# transform classpath into a comma separated list
LIBJARS=${CLASSPATH//:/,}


#==========================================
# ingest parameters
declare -r _today=$(date "+%Y%m%d%H%M%S")
declare _argDate=$(date -d "now" "+%Y%m%d")
declare -r _workDir=${WAREHOUSE_NAME_BASE_DIR}${BASE_WORK_DIR}/${_today}-$$/
if (($# >= 1)); then
    _argDate=$1
    if [[ -n "$2" ]]; then
        declare _NumShards=$2
    fi
fi

#================================================
# initialize map/reduce JVM settings
test -n "${_HeapDump}" && declare -r _heap="-XX:-HeapDumpOnOutOfMemoryError"
declare -r _DefaultJVMArgs="-XX:+UseConcMarkSweepGC \
-Dfile.encoding=UTF8 \
-Duser.timezone=GMT \
-XX:+UseNUMA \
${_heap} \
-XX:NewRatio=2"
declare -r _dateStr=$(date "+%Y_%m_%d-%H_%M_%S")
declare -r _GCLogOpts="-XX:+PrintGC \
-XX:+PrintGCTimeStamps \
-XX:+PrintGCDateStamps \
-XX:+PrintGCDetails"
# set mapper/reducer log directory
_GCLogDir="${_GCLogDir:-/tmp}"

# set in memory sort io buffer size
declare -i _MapInMemSortBufferMBSize
declare _StatsOpts
declare _ChildMapOpts
declare _ChildReduceOpts
setMapperJVMParameters
setReducerJVMParameters
setStatsParameters

declare _NumReducers
calculateReducers _NumReducers

#============================================
# Hadoop Job JVM Options
export HADOOP_CLASSPATH=$CLASSPATH
export HADOOP_OPTS="${_DefaultJVMArgs}"
# for remote debug of job
# export HADOOP_CLIENT_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8989"

declare -i TIMEOUT=600000

# map/reduce settings see Hadoop Map/Reduce properties
_MapReduceOpts="-mapreduce.task.io.sort.mb=${_MapInMemSortBufferMBSize} \
-mapreduce.task.io.sort.factor=${BULK_CHILD_IO_SORT_MB} \
-compressionType gz \
-mapreduce.map.output.compression.codec=${BULK_MAP_OUTPUT_COMPRESSION_CODEC} \
-mapreduce.map.output.compress=${BULK_MAP_OUTPUT_COMPRESS} \
-mapreduce.output.fileoutputformat.compress=true \
-mapreduce.output.fileoutputformat.compress.type=${BULK_MAP_OUTPUT_COMPRESSION_TYPE} \
-bulk.ingest.mapper.threads=0 \
-bulk.ingest.mapper.workqueue.size=10000 \
-io.file.buffer.size=1048576 \
-dfs.bytes-per-checksum=4096 \
-io.sort.record.percent=.10 \
-mapreduce.map.sort.spill.percent=.50 \
-mapreduce.job.reduces=${_NumReducers} \
-mapreduce.task.timeout=${TIMEOUT} \
-markerFileReducePercentage 0.33 \
-context.writer.max.cache.size=2500 \
-mapreduce.job.queuename=bulkIngestQueue \
-sequenceialLocationPartitioner \
-dfs.replication=3 \
"

BATCHWRITER_OPTS="-AccumuloOutputFormat.WriteOpts.BatchWriterConfig=    11#maxMemory=100000000,maxWriteThreads=4"

# define the location of the jar files for map/reduce job
declare -r _jarCacheDir=${WAREHOUSE_NAME_BASE_DIR}${JOB_CACHE_DIR}

declare -r _hdfsDir=${WAREHOUSE_NAME_BASE_DIR:-${WAREHOUSE_HDFS_NAME_NODE}}


# define job ingest parmeters
declare -r _IngestOpts="\
-cacheBaseDir ${_jarCacheDir} \
-cacheJars ${LIBJARS} \
-user $USERNAME \
-pass $PASSWORD \
-instance ${WAREHOUSE_INSTANCE_NAME} \
-zookeepers ${WAREHOUSE_ZOOKEEPERS} \
-workDir ${_workDir} \
-flagFileDir ${FLAG_DIR} \
-flagFilePattern '.*_(indexstats)_*\.flag' \
-srcHdfs ${_hdfsDir} \
-destHdfs ${_hdfsDir} \
-skipMarkerFileGeneration \
-writeDirectlyToDest \
-tableCounters \
-contextWriterCounters \
-disableRefreshSplits \
-distCpConfDir ${WAREHOUSE_HADOOP_CONF} \
-partitioner.default.delegate=datawave.ingest.mapreduce.partition.MultiTableRRRangePartitioner \
-datawave-ingest.splits.cache.dir=${WAREHOUSE_NAME_BASE_DIR}/data/splitsCache \
-BulkInputFormat.working.dir=${WAREHOUSE_NAME_BASE_DIR}/tmp/shardStats \
-ingestMetricsDisabled \
"
# replace -disableRefreshSplits for local testing or run generate-splits-file.sh
# -jobGeneratedSplits \


set -x
${WAREHOUSE_HADOOP_HOME}/bin/hadoop --config ${WAREHOUSE_HADOOP_CONF} \
jar ${DATAWAVE_INDEX_STATS_JAR} datawave.mapreduce.shardStats.StatsJob \
-jt ${WAREHOUSE_JOBTRACKER_NODE} ${_argDate} \
${_IngestConfig[@]} \
-mapreduce.map.java.opts="${_ChildMapOpts}" \
-mapreduce.reduce.java.opts="${_ChildReduceOpts}" \
${_IngestOpts} "${BATCHWRITER_OPTS}" ${_MapReduceOpts} ${_StatsOpts}
declare _rc=$?
set +x
cleanup $_rc

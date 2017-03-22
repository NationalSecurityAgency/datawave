#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
	THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
	THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

INPUT_DIRS=$1
TYPE=$2
OUTPUT_DIRS=$3
EXTRA_OPTS=${@:4}

# Split the instance off of the TYPE
DATATYPE=${TYPE%.*}

. ../ingest/ingest-env.sh

#
# Poller setup
#
MANAGER="-m nsa.datawave.ingest.poller.manager.MultiThreadedEventSequenceFileCombiningPollManager"
MANAGER_THREADS=${POLLER_THREADS}
FILE_FILTER="nsa.datawave.poller.filter.DataTypeFileFilter"
FILE_FILTER_CONFIG="poller.xml"
ERROR_DIRS="$OUTPUT_DIRS/errors"
QUEUE_DIRS="$OUTPUT_DIRS/queue"
ARCHIVE_DIRS="$OUTPUT_DIRS/archive"
COMPLETED_DIR="$OUTPUT_DIRS/completed"
WORK_DIR="$OUTPUT_DIRS/work"
METRICS_DIR="$OUTPUT_DIRS/metrics"
BASE_MEM=512
MEM_PER_THREAD=75

HADOOP_NATIVE_LIB_DIR="$INGEST_HADOOP_HOME/lib/native"

if [[ -z $DATAWAVE_INGEST_HOME ]]; then
  export DATAWAVE_INGEST_HOME=$THIS_DIR/../..
fi


# if the extra options contain a clientId, then assume we are using the distributed dedupping mechanism
if [[ "$EXTRA_OPTS" =~ ".*-clientId .*" || "$EXTRA_OPTS" =~ "-clientId " ]]; then
  MANAGER="-m nsa.datawave.ingest.poller.manager.distributed.DeduppingEventSequenceFileCombiningPollManager"
  # set the ageoff to be 1/2 a year
  ageoff=`echo "( 365.2425 / 2 ) * 24 * 60 * 60 * 1000" | bc`
  # set the max ownership wait to be 30 seconds before ownership is assumed by a non-leader
  ownwait=`echo "30 * 1000" | bc`
  EXTRA_OPTS="$EXTRA_OPTS -user $USERNAME -pass $PASSWORD -instance $INGEST_INSTANCE_NAME -zookeepers $INGEST_ZOOKEEPERS -registry $DATATYPE -registryTable pollerUidRegistry -ageOffMs $ageoff -ownMs $ownwait"
fi

# if the extra options contain a baseDir redefinition, then pick it up as the BASE_DIR
BASE_DIR=$HDFS_BASE_DIR
if [[ "$EXTRA_OPTS" =~ ".*-baseDir .*" || "$EXTRA_OPTS" =~ "-baseDir " ]]; then
    BASE_DIR=${EXTRA_OPTS##*-baseDir }
    BASE_DIR=${BASE_DIR%% *}
    EXTRA_OPTS=${EXTRA_OPTS/-baseDir $BASE_DIR/}
fi

# if the extra options contains a manager thread count redefinition, then pick it up as the MANAGER_THREADS
if [[ "$EXTRA_OPTS" =~ ".*-mc .*" || "$EXTRA_OPTS" =~ "-mc " ]]; then
    MANAGER_THREADS=${EXTRA_OPTS##*-mc }
    MANAGER_THREADS=${MANAGER_THREADS%% *}
    EXTRA_OPTS=${EXTRA_OPTS/-mc $MANAGER_THREADS/}
fi

#
# EventSequenceFileCombiningPollManager setup
#
KEY_CLASS="org.apache.hadoop.io.LongWritable"
VALUE_CLASS="org.apache.hadoop.io.Text"
OUT_KEY_CLASS="org.apache.hadoop.io.LongWritable"
OUT_VALUE_CLASS="nsa.datawave.ingest.config.RawRecordContainerImpl"

# determine the configuration files to load
CONF_FILES=all-config.xml,ingest-config.xml
DATATYPE_CONF_FILE=""
for (( count=0; count < ${#CONFIG_DATA_TYPES[@]}; count=$((count + 1)) )); do
    if [[ "$DATATYPE" == "${CONFIG_DATA_TYPES[$count]}" ]]; then
	DATATYPE_CONF_FILE=${CONFIG_FILES[$count]}
    fi
done
if [[ "$DATATYPE_CONF_FILE" == "" ]]; then
    echo "Could not find configuration file for $DATATYPE"
    exit -1
fi
echo "Using ${DATATYPE_CONF_FILE} for ${DATATYPE} configuration"
CONF_FILES=${CONF_FILES},${DATATYPE_CONF_FILE}


# append the manager specific options
MANAGER="$MANAGER -ikc $KEY_CLASS -ivc $VALUE_CLASS -okc $OUT_KEY_CLASS -ovc $OUT_VALUE_CLASS -cf $CONF_FILES -md $METRICS_DIR"

# now if we determine that the extra options are specifying an alternative manager, then undo this manager configuration
if [[ "$EXTRA_OPTS" =~ ".*-m .*" || "$EXTRA_OPTS" =~ "-m " ]]; then
    MANAGER=
fi

MAX_MEM=$((BASE_MEM + MANAGER_THREADS * MEM_PER_THREAD))
if [[ "$EXTRA_OPTS" =~ ".*-maxMB .*" || "$EXTRA_OPTS" =~ "-maxMB " ]]; then
    MAX_MEM=${EXTRA_OPTS##*-maxMB }
    MAX_MEM=${MAX_MEM%% *}
    EXTRA_OPTS=${EXTRA_OPTS/-maxMB $MAX_MEM/}
fi
export JAVA_OPTS="-Xmx${MAX_MEM}m -Xms128m -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 -Djava.library.path=$HADOOP_NATIVE_LIB_DIR -Dfile.encoding=UTF8 -Duser.timezone=GMT $CHILD_INGEST_OPTS"

# no max of files to merge
MAX_FILES=0

BLOCKSIZEMB=""
if [[ "$POLLER_FILE_BLOCK_SIZE_MB" != "" ]]; then
  BLOCKSIZEMB="-b $POLLER_FILE_BLOCK_SIZE_MB"
fi
EVENTSIZEMB=""
if [[ "$POLLER_MAX_TOTAL_EVENT_OUTPUT_MB" != "" ]]; then
  EVENTSIZEMB="-ut $POLLER_MAX_TOTAL_EVENT_OUTPUT_MB"
fi
RECORDS=""
if [[ "$POLLER_MAX_OUTPUT_RECORDS" != "" ]]; then
  RECORDS="-rt $POLLER_MAX_OUTPUT_RECORDS"
fi

# Turn some of the comma delimited lists into arrays
OLD_IFS="$IFS"
IFS=","
FIFTEEN_MIN_INGEST_DATA_TYPES=( $FIFTEEN_MIN_INGEST_DATA_TYPES )
FIVE_MIN_INGEST_DATA_TYPES=( $FIVE_MIN_INGEST_DATA_TYPES )
IFS="$OLD_IFS"

### use the ONE_HR configuration as the default

# Poll directory every 60 seconds
INTERVAL=60000

# Close output file every 5 minutes
LATENCY=300000

if [[ "$POLLER_ONE_HR_FILE_BLOCK_SIZE_MB" != "" ]]; then
  BLOCKSIZEMB="-b $POLLER_ONE_HR_FILE_BLOCK_SIZE_MB"
fi
if [[ "$POLLER_ONE_HR_MAX_TOTAL_EVENT_OUTPUT_MB" != "" ]]; then
  EVENTSIZEMB="-ut $POLLER_ONE_HR_MAX_TOTAL_EVENT_OUTPUT_MB"
fi
if [[ "$POLLER_ONE_HR_MAX_OUTPUT_RECORDS" != "" ]]; then
  RECORDS="-rt $POLLER_ONE_HR_MAX_OUTPUT_RECORDS"
fi


### override with the fifteen minute configuration if so configured
for (( count=0; count < ${#FIFTEEN_MIN_INGEST_DATA_TYPES[@]}; count=$((count + 1)) )); do
  if [[ "${FIFTEEN_MIN_INGEST_DATA_TYPES[$count]}" == "$DATATYPE" ]]; then
    # Poll directory every 20 seconds
    INTERVAL=20000

    # Close output file every 60 seconds
    LATENCY=60000

    if [[ "$POLLER_FIFTEEN_MIN_FILE_BLOCK_SIZE_MB" != "" ]]; then
      BLOCKSIZEMB="-b $POLLER_FIFTEEN_MIN_FILE_BLOCK_SIZE_MB"
    fi
    if [[ "$POLLER_FIFTEEN_MIN_MAX_TOTAL_EVENT_OUTPUT_MB" != "" ]]; then
      EVENTSIZEMB="-ut $POLLER_FIFTEEN_MIN_MAX_TOTAL_EVENT_OUTPUT_MB"
    fi
    if [[ "$POLLER_FIFTEEN_MIN_MAX_OUTPUT_RECORDS" != "" ]]; then
      RECORDS="-rt $POLLER_FIFTEEN_MIN_MAX_OUTPUT_RECORDS"
    fi
  fi
done

### override with the five minute configuration if so configured
for (( count=0; count < ${#FIVE_MIN_INGEST_DATA_TYPES[@]}; count=$((count + 1)) )); do
  if [[ "${FIVE_MIN_INGEST_DATA_TYPES[$count]}" == "$DATATYPE" ]]; then
    # Poll directory every 2 seconds
    INTERVAL=2000

    # Close output file every 20 seconds
    LATENCY=20000

    if [[ "$POLLER_FIVE_MIN_FILE_BLOCK_SIZE_MB" != "" ]]; then
      BLOCKSIZEMB="-b $POLLER_FIVE_MIN_FILE_BLOCK_SIZE_MB"
    fi
    if [[ "$POLLER_FIVE_MIN_MAX_TOTAL_EVENT_OUTPUT_MB" != "" ]]; then
      EVENTSIZEMB="-ut $POLLER_FIVE_MIN_MAX_TOTAL_EVENT_OUTPUT_MB"
    fi
    if [[ "$POLLER_FIVE_MIN_MAX_OUTPUT_RECORDS" != "" ]]; then
      RECORDS="-rt $POLLER_FIVE_MIN_MAX_OUTPUT_RECORDS"
    fi
  fi
done

HDFS_NAME_NODE=$INGEST_HDFS_NAME_NODE
if [[ "$EXTRA_OPTS" =~ ".*-warehouse.*" || "$EXTRA_OPTS" =~ "-warehouse" ]]; then
    HDFS_NAME_NODE=$WAREHOUSE_HDFS_NAME_NODE
    EXTRA_OPTS=${EXTRA_OPTS/-warehouse/}
fi

./poller.sh -d $INPUT_DIRS -w $WORK_DIR -t $TYPE $MANAGER -mc $MANAGER_THREADS -f $FILE_FILTER -fc $FILE_FILTER_CONFIG -e $ERROR_DIRS -h ${HDFS_NAME_NODE}${BASE_DIR} -i $INTERVAL -l $LATENCY $BLOCKSIZEMB $EVENTSIZEMB $RECORDS -mf $MAX_FILES -q $QUEUE_DIRS -r $COMPLETED_DIR -a $ARCHIVE_DIRS $EXTRA_OPTS

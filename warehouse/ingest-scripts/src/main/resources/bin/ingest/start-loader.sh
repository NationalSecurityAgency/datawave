#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
	THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
	THIS_SCRIPT=`readlink -f $0`
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

. ../ingest/ingest-env.sh

if [[ -z ${LOCK_FILE_DIR} ]]; then
  echo "LOCK_FILE_DIR is not set, be sure to source bin/ingest/ingest-env.sh"
  exit -1
fi

# If the paused file exists, then prevent startup unless forcing
if [[ "$@" =~ ".*-force.*" || "$@" =~ "-force" ]]; then
  rm -f ${LOCK_FILE_DIR}/LOADER_STARTUP.LCK
  $0 ${@/-force/}
  exit $?
fi
if [ -e ${LOCK_FILE_DIR}/LOADER_STARTUP.LCK ]; then
  echo "Startup has been locked out.  Use -force to unlock."
  exit -1
fi

MAPFILE_LOADER_CMD=$THIS_DIR/map-file-bulk-loader.sh
COUNT=$($MAPFILE_LOADER_COMMAND_PREFIX pgrep -f "\-Dapp=bulkIngestMapFileLoader" -c)

# Sum the number of loaders for all the name nodes
EXPECTED_COUNT=0
for (( LOADER=0; LOADER < ${#NUM_MAP_LOADERS[@]}; LOADER=$((LOADER + 1)) )); do
  EXPECTED_COUNT=$((EXPECTED_COUNT+NUM_MAP_LOADERS[$LOADER]))
done

function resetJobLoadingFile () {
  NAME_NODE=$1
  WORK_DIR=$2
  FILES_STUCK_LOADING=$($INGEST_HADOOP_HOME/bin/hadoop fs -ls "${NAME_NODE}${WORK_DIR}/*/job.loading" | awk '{print $NF}')
  if [[ ! -z $FILES_STUCK_LOADING ]]; then
    for stuckFile in $FILES_STUCK_LOADING; do
      echo "Resetting ${stuckFile}"
      moving_result=$($INGEST_HADOOP_HOME/bin/hadoop fs -mv $stuckFile ${stuckFile%.loading}.complete 2>&1)
      if [[ ! -z $moving_result ]]; then
        echo "Error resetting file: $moving_result . Manually check for orphans."
      fi
    done
  fi
}

# MAP_LOADER_HDFS_NAME_NODES and NUM_MAP_LOADERS are two bash arrays that should have an equal number of entries

if [[ COUNT -eq 0 ]]; then
  for (( LOADER=0; LOADER < ${#MAP_LOADER_HDFS_NAME_NODES[@]}; LOADER=$((LOADER + 1)) )); do
    COUNT=${NUM_MAP_LOADERS[$LOADER]}
    MAP_LOADER_HDFS_NAME_NODE=${MAP_LOADER_HDFS_NAME_NODES[$LOADER]}
    MAP_LOADER_WORKDIR=${MAP_LOADER_HDFS_DIRS[$LOADER]}

    resetJobLoadingFile ${MAP_LOADER_HDFS_NAME_NODE} ${MAP_LOADER_WORKDIR}

    echo "starting $COUNT map file loaders for ${MAP_LOADER_WORKDIR} on ${MAP_LOADER_HDFS_NAME_NODE} ..."
    for (( ; $COUNT; COUNT = $((COUNT-1)) )); do
      $MAPFILE_LOADER_CMD -srcHDFS ${MAP_LOADER_HDFS_NAME_NODE} -destHDFS ${MAP_LOADER_HDFS_NAME_NODE} -shutdownPort "241$LOADER$COUNT" >> $LOG_DIR/map-file-loader.$LOADER$COUNT.log 2>&1 &
    done
  done

  #Comments about run extra map loader
  if [[ -z ${EXTRA_MAP_LOADER} ]]; then
    echo "No extra map file loader configured."
  else
    # Set loader to use the number after the previous loader
    LOADER=${#MAP_LOADER_HDFS_NAME_NODES[@]}
    COUNT=0
    export MAP_LOADER_WORKDIR=${BASE_WORK_DIR}
    echo "start 1 map file loader for ${MAP_LOADER_WORKDIR} on ${EXTRA_MAP_LOADER}..."
    $MAPFILE_LOADER_CMD -srcHdfs ${EXTRA_MAP_LOADER} -destHdfs ${EXTRA_MAP_LOADER} -shutdownPort "231$LOADER$COUNT" >> $LOD_DIR/map-file-loader.$LOADDER$COUNT.log 2>&1 &
  fi

  if [[! -z $MAP_LOADER_CUSTOM ]]; then
    for ((CUSTOM_LOADER=0; CUSTOM_LOADER < ${#MAP_LOADER_CUSTOM[@]}; CUSTOM_LOADER=$((CUSTOM_LOADER +1)) )); do
      echo "starting additional map file loader: ${MAP_LOADER_CUSTOM[$CUSTOM_LOADER]}"
      ${MAP_LOADER_CUSTOM[$CUSTOM_LOADER]} -shutdownPort "2510$CUSTOM_LOADER" >> $LOG_DIR/map-file-loader-custom.$CUSTOM_LOADER.log 2>&1 &
    done
  fi
elif [[ COUNT -ne EXPECTED_COUNT ]]; then
  echo "Expected $EXPECTED_COUNT but saw $COUNT."
  read -p "Attempt to start missing map file loaders? (y/n) " -n 1 -r REPLY
  echo
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Comparing map file loaders to configuration..."
    for (( LOADER=0; LOADER < ${#MAP_LOADER_HDFS_NAME_NODES[@]}; LOADER=$((LOADER + 1)) )); do
      COUNT=${NUM_MAP_LOADERS[$LOADER]}
      NUM_RUNNING_FOR_THIS_NAME_NODE=$($MAPFILE_LOADER_COMMAND_PREFIX pgrep -f "\-Dapp=bulkIngestMapFileLoader.*${MAP_LOADER_HDFS_NAME_NODES[$LOADER]}" -c)

      echo "${NUM_RUNNING_FOR_THIS_NAME_NODE} of ${COUNT} map file loaders running for ${MAP_LOADER_HDFS_NAME_NODES[$LOADER]}."

      if [[ $NUM_RUNNING_FOR_THIS_NAME_NODE -eq 0 ]]; then
        resetJobLoadingFile ${MAP_LOADER_HDFS_NAME_NODES[$LOADER]} ${MAP_LOADER_WORKDIR[$LOADER]}
      elif  [[ $NUM_RUNNING_FOR_THIS_NAME_NODE -lt $COUNT ]]; then
        echo "Attempting to start missing map file loaders..."
        for (( ; $COUNT; COUNT=$((COUNT-1)) )) ; do
          SHUTDOWN_PORT="241${LOADER}${COUNT}"

          # look at process in the wild
          NUM_RUNNING_ON_THIS_PORT=$(pgrep -f "shutdownPort\s+['\"]?${SHUTDOWN_PORT}['\"]?$" -c)
          NUM_RUNNING_ON_THIS_PORT=$(pgrep -f "shutdownPort\s+['\"]?${SHUTDOWN_PORT}['\"]?\s+" -c)
          NUM_RUNNING_ON_THIS_PORT=$(pgrep -f "shutdownPort\s*['\"]?${SHUTDOWN_PORT}" -c)
          if [[ ${NUM_RUNNING_ON_THIS_PORT} -eq 0 ]]; then
            echo "Identified missing map file loader, starting with shutdown port ${SHUTDOWN_PORT}."
            $MAPFILE_LOADER_CMD -srcHdfs ${MAP_LOADER_HDFS_NAME_NODES[$LOADER]} -destHdfs ${MAP_LOADER_HDFS_NAME_NODES[$LOADER]} -shutdownPort "${SHUTDOWN_PORT}" >> $LOG_DIR/map-file-loader.$LOADER$COUNT.log 2>&1 &
          fi
        done
      fi
    done
else
  echo "$COUNT map file loaders already running"
fi




#MAP_LOADER_HDFS_NAME_NODES=( hdfs://localhost/ hdfs://localhost/ hdfs://localhost/)
#NUM_MAP_LOADERS=( 1 2 3 )
MAP_LOADER_HDFS_NAME_NODES=( hdfs://localhost/ )
NUM_MAP_LOADERS=( 1 )
#echo $EXPECTED_COUNT

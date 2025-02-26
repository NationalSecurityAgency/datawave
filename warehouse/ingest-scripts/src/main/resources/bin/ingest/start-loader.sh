#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

. ../ingest/ingest-env.sh

if [[ -z ${LOCK_FILE_DIR} ]]; then
  echo "LOCK_FILE_DIR is not set, be sure to source bin/ingest/ingest-env.sh"
  exit -1
fi

# If the paused file exists, then prevent startup unless forcing
if [[ "$@" =~ .*-force.* || "$@" =~ "-force" ]]; then
  rm -f ${LOCK_FILE_DIR}/LOADER_STARTUP.LCK
  $0 ${@/-force/}
  exit $?
fi
if [ -e ${LOCK_FILE_DIR}/LOADER_STARTUP.LCK ]; then
  echo "Startup has been locked out.  Use -force to unlock."
  exit -1
fi

PATH="${PATH}:/usr/sbin"
export PATH

MAPFILE_LOADER_CMD=$THIS_DIR/map-file-bulk-loader.sh
PIDS=$($MAPFILE_LOADER_COMMAND_PREFIX pgrep -f "\-Dapp=bulkIngestMapFileLoader")
COUNT=0
IFS=';' read -r -a values <<<$MAP_LOADER_HDFS_NAME_NODES_CONFIG
warehouse_values=()
warehouse_numbers=()
for v in "${values[@]}"
do
  IFS=',' read -r -a v2 <<<$v
  warehouse_values+=(${v2[0]})
  warehouse_numbers+=(${v2[1]})
done
EXTRA_MAP=0
if [[ -n $EXTRA_MAP_LOADER ]]; then
  EXTRA_MAP=1
fi
NUM_MAP_LOADERS_COPY=()
for c in "${NUM_MAP_LOADERS[@]}"
do
  NUM_MAP_LOADERS_COPY+=(${c})
done

for PID in $PIDS; do
  COUNT=$((COUNT + 1))
  warehouse_current=$(ps -p $PID -o command --no-headers | awk -F "-srcHdfs" '{print $3}' | cut -d " " -f 2)
  if [[ "$EXTRA_MAP_LOADER" == "${warehouse_current}" && $EXTRA_MAP != 0 ]]; then
    EXTRA_MAP=0
  fi

  for ((count = 0; count < ${#MAP_LOADER_HDFS_NAME_NODES[@]}; count = $((count + 1))));
  do
      if [[ "$warehouse_current" == "${warehouse_values[$count]}" ]]; then
        NUM_MAP_LOADERS_COPY[$count]=$((NUM_MAP_LOADERS_COPY[$count] - 1))
      fi
  done
done

TOTAL=0
for i in ${NUM_MAP_LOADERS_COPY[@]}; do
  TOTAL=$((TOTAL + i))
done
TOTAL=$((TOTAL + EXTRA_MAP))

if [[ ${TOTAL} -gt 0 ]]; then
  SHUTDOWN_PORT=24100
  for ((LOADER = 0; LOADER < ${#MAP_LOADER_HDFS_NAME_NODES[@]}; LOADER = $((LOADER + 1)))); do
    #Make sure that there are no active loaders running on the namenode in question before "resetting" perceived stuck files in HDFS
    currentLoaders=$(ps -eaf | grep "[b]ulkIngestMap" | grep ${MAP_LOADER_HDFS_NAME_NODES[$LOADER]} | cut -d" " -f1-7)
    echo "currentLoaders is: " $currentLoaders
    FILES_STUCK_LOADING=$(${INGEST_HADOOP_HOME}/bin/hadoop fs -ls "${MAP_LOADER_HDFS_NAME_NODES[$LOADER]}$BASE_WORK_DIR/*/job.loading" | awk '{print $NF}')
    if [[ -n $FILES_STUCK_LOADING && -z $currentLoaders ]]; then
      echo "About to reset stuck files, no active loaders detected on ${MAP_LOADER_HDFS_NAME_NODES[$LOADER]}"
      for stuckFile in $FILES_STUCK_LOADING; do
        echo "Resetting ${stuckFile} to ${stuckFile%.loading}.complete"
        moving=$(${INGEST_HADOOP_HOME}/bin/hadoop fs -mv $stuckFile ${stuckFile%.loading}.complete 2>&1)
        if [[ -n $moving ]]; then
          echo "Error resetting file: $moving . Manually check for orphans."
        fi
      done
    fi
    COUNT=${NUM_MAP_LOADERS_COPY[$LOADER]}
    MAP_LOADER_HDFS_NAME_NODE=${MAP_LOADER_HDFS_NAME_NODES[$LOADER]}
    export MAP_LOADER_WORKDIR=${MAP_LOADER_HDFS_DIRS[$LOADER]}
    echo "starting $COUNT map file loaders for ${MAP_LOADER_HDFS_NAME_NODE} ..."
    portInUse=$(lsof -i:${SHUTDOWN_PORT} | grep $SHUTDOWN_PORT)
    portUsed=$(ps -eaf | grep "[b]ulkIngestMap" | grep $SHUTDOWN_PORT)
    for (( x=0; x < COUNT; x=$((x+1)) )) ; do
      while [[ -n "$portInUse$portUsed" ]]
      do
          echo "port in use, finding another"
          SHUTDOWN_PORT=$((SHUTDOWN_PORT + 1))
          portInUse=$(lsof -i:${SHUTDOWN_PORT} | grep $SHUTDOWN_PORT)
          portUsed=$(ps -eaf | grep "[b]ulkIngestMap" | grep $SHUTDOWN_PORT)
      done
      echo starting map file loader with log file map-file-loader.$LOADER$x.log
      $MAPFILE_LOADER_CMD -srcHdfs ${MAP_LOADER_HDFS_NAME_NODE} -destHdfs ${MAP_LOADER_HDFS_NAME_NODE} -shutdownPort ${SHUTDOWN_PORT} >> $LOG_DIR/map-file-loader.$LOADER$x.log 2>&1 &
      SHUTDOWN_PORT=$((SHUTDOWN_PORT + 1))
      portInUse=$(lsof -i:${SHUTDOWN_PORT} | grep $SHUTDOWN_PORT)
      portUsed=$(ps -eaf | grep "[b]ulkIngestMap" | grep $SHUTDOWN_PORT)
      done

  done
  # Run an extra map loader, if defined.
  # This may be used as a safeguard in case the warehouse name node is not in the main map loader property.
  # It can also help with transitioning to different loader settings.
  if [[ -z ${EXTRA_MAP_LOADER} ]]; then
    echo "No extra map file loader configured."
  elif [[ ${EXTRA_MAP} == 0 ]]; then
    echo "Extra map loader already running."
  else
    # set LOADER to use the number after the previous loader
    LOADER=${#MAP_LOADER_HDFS_NAME_NODES[@]}
    COUNT=0
    export MAP_LOADER_WORKDIR=${BASE_WORK_DIR}
    echo "starting 1 extra map file loader for ${EXTRA_MAP_LOADER} ..."
    SHUTDOWN_PORT=$((SHUTDOWN_PORT + 1))
    portInUse=$(lsof -i:${SHUTDOWN_PORT} | grep $SHUTDOWN_PORT)
    portUsed=$(ps -eaf | grep "[b]ulkIngestMap" | grep $SHUTDOWN_PORT)
    while [[ -n "$portInUse$portUsed" ]]
    do
        SHUTDOWN_PORT=$((SHUTDOWN_PORT + 1))
        portInUse=$(lsof -i:${SHUTDOWN_PORT} | grep $SHUTDOWN_PORT)
        portUsed=$(ps -eaf | grep "[b]ulkIngestMap" | grep $SHUTDOWN_PORT)
    done
    echo starting map file loader with log file map-file-loader.$LOADER$COUNT.log
    $MAPFILE_LOADER_CMD -srcHdfs ${EXTRA_MAP_LOADER} -destHdfs ${EXTRA_MAP_LOADER} -shutdownPort ${SHUTDOWN_PORT} >>$LOG_DIR/map-file-loader.$LOADER$COUNT.log 2>&1 &
  fi

  if [[ ! -z $MAP_LOADER_CUSTOM ]]; then
    for ((CUSTOM_LOADER = 0; CUSTOM_LOADER < ${#MAP_LOADER_CUSTOM[@]}; CUSTOM_LOADER = $((CUSTOM_LOADER + 1)))); do
      echo "starting additional map file loader: ${MAP_LOADER_CUSTOM[$CUSTOM_LOADER]}"
      SHUTDOWN_PORT=25100
      portInUse=$(lsof -i:${SHUTDOWN_PORT} | grep $SHUTDOWN_PORT)
      portUsed=$(ps -eaf | grep "[b]ulkIngestMap" | grep $SHUTDOWN_PORT)
      while [[ ! -z "$portInUse$portUsed" ]]
      do
          SHUTDOWN_PORT=$((SHUTDOWN_PORT + 1))
          portInUse=$(lsof -i:${SHUTDOWN_PORT} | grep $SHUTDOWN_PORT)
          portUsed=$(ps -eaf | grep "[b]ulkIngestMap" | grep $SHUTDOWN_PORT)
      done
      echo starting map file loader with log file $LOG_DIR/map-file-loader-custom.$CUSTOM_LOADER.log
      ${MAP_LOADER_CUSTOM[$CUSTOM_LOADER]} -shutdownPort ${SHUTDOWN_PORT} >>$LOG_DIR/map-file-loader-custom.$CUSTOM_LOADER.log 2>&1 &
      done
  fi
else
      echo "$COUNT map file loaders already running"
fi

#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f $0)
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
PIDS=$($MAPFILE_LOADER_COMMAND_PREFIX pgrep -f "\-Dapp=bulkIngestMapFileLoader")
COUNT=0
IFS=';' read -a values <<<$MAP_LOADER_HDFS_NAME_NODES_CONFIG
warehouse_values=()
warehouse_numbers=()
for v in "${values[@]}"; do
  IFS=',' read -a v2 <<<$v
  warehouse_values+=(${v2[0]})
  warehouse_numbers+=(${v2[1]})
done
EXTRA_MAP=0
if [[ ! -z $EXTRA_MAP_LOADER ]]; then
  EXTRA_MAP=1
fi
NUM_MAP_LOADERS_COPY=()
for c in "${NUM_MAP_LOADERS[@]}"; do
  NUM_MAP_LOADERS_COPY+=(${c})
done

for PID in $PIDS; do
  COUNT=$((COUNT + 1))
  warehouse_current=$(ps -p $PID -o command --no-headers | awk -F "-srcHdfs '{print $3}' | cut -d ' ' -f2")
  if [[ "$EXTRA_MAP_LOADER" == "${warehouse_current}" && EXTRA_MAP != 0 ]]; then
    EXTRA_MAP=0
  fi

  for ((count = 0; count < ${#MAP_LOADER_HDFS_NAME_NODES[@]}; count = $((count + 1)))); do
    echo "warehouse value : ${warehouse_values[$count]}"
    if [[ "$warehouse_current" == "$warehouse_values[$count]}" ]]; then
      NUM_MAP_LOADERS_COPY[$count]=$((NUM_MAP_LOADERS_COPY[$count] - 1))
    fi
  done
done

TOTAL=0
for i in ${NUM_MAP_LOADERS_COPY[@]}; do
  TOTAL=$((TOTAL + i))
done
TOTAL=$((TOTAL + EXTRA_MAP))

if [[${TOTAL} >0 ]]; then
  for ((LOADER = 0; LOADER < ${#MAP_LOADER_HDFS_NAME_NODES[@]}; LOADER = $((LOADER + 1)))); do
    FILES_STUCK_LOADING=$($INGEST_HADOOP_HOME/bin/hadoop fs -ls "${MAP_LOADER_HDFS_NAME_NODES[$LOADER]}$BASE_WORK_DIR/*/job.loading" | awk '{print $NF}')
    if [[ ! -z $FILES_STUCK_LOADING ]]; then
      for stuckFile in $FILES_STUCK_LOADING; do
        echo "Resetting ${stuckFile}"
        moving_result=$($INGEST_HADOOP_HOME/bin/hadoop fs -mv $stuckFile ${stuckFile%.loading}.complete 2>&1)
        if [[ ! -z $moving_result ]]; then
          echo "Error resetting file: $moving_result . Manually check for orphans."
        fi
      done
    fi
    COUNT=${NUM_MAP_LOADERS_COPY[$LOADER]}
    echo "starting $COUNT map file loaders for ${MAP_LOADER_HDFS_NAME_NODES[$LOADER]} ..."
    SHUTDOWN_PORT=24100
    portInUse=$(ps -eaf | grep [b]ulkIngestMap | grep $SHUTDOWN_PORT)
    while [[ ! -z $portInUse ]]; do
      SHUTDOWN_PORT=$((SHUTDOWN_PORT + 1))
      portInUse=$(ps -eaf | grep [b]ulkIngestMap | grep $SHUTDOWN_PORT)
    done
    for ((x = 0; x < $COUNT; x = $((x + 1)))); do
      $MAPFILE_LOADER_CMD -srcHdfs ${MAP_LOADER_HDFS_NAME_NODE} -destHdfs ${MAP_LOADER_HDFS_NAME_NODE} -shutdownPort ${SHUTDOWN_PORT} >>$LOG_DIR/map-file-loader.$LOADER$COUNT.log 2>&1 &
      SHUTDOWN_PORT=$((SHUTDOWN_PORT + 1))
      portInUse=$(ps -eaf | grep [b]ulkIngestMap | grep $SHUTDOWN_PORT)
    done

  done
  # Run an extra map loader, if defined.
  # This may be used as a safeguard in case the name node is not in the main map loader property.
  if [[ -z ${EXTRA_MAP_LOADER} ]]; then
    echo "No extra map file loader configured"
  elif [[ ${EXTRA_MAP} == 0 ]]; then
    echo "Extra map loader already running."
  else
    # set LOADER to use the number after the previous one.
    LOADER=${#MAP_LOADER_HDFS_NAME_NODES[@]}
    COUNT=0
    export MAP_LOADER_WORKDIR=${BASE_WORK_DIR}
    echo "starting 1 map file loader for ${MAP_LOADER_WORKDIR} on ${EXTRA_MAP_LOADER} ..."
    SHUTDOWN_PORT=$((SHUTDOWN_PORT + 1))
    portInUse=$(ps -eaf | grep [b]ulkIngestMap | grep $SHUTDOWN_PORT)
    while [[ ! -z $portInUse ]]; do
      SHUTDOWN_PORT=$((SHUTDOWN_PORT + 1))
      portInUse=$(ps -eaf | grep [b]ulkIngestMap | grep $SHUTDOWN_PORT)
    done

    $MAPFILE_LOADER_CMD -srcHdfs ${EXTRA_MAP_LOADER} -destHdfs ${EXTRA_MAP_LOADER} -shutdownPort ${SHUTDOWN_PORT} >>$LOG_DIR/map-file-loader.$LOADER$COUNT.log 2>&1 &
  fi

  if [[ ! -z MAP_LOADER_CUSTOM ]]; then
    for ((CUSTOM_LOADER = 0; CUSTOM_LOADER < ${#MAP_LOADER_CUSTOM[@]}; CUSTOM_LOADER = $((CUSTOM_LOADER + 1)))); do
      echo "starting additional map file loader: ${MAP_LOADER_CUSTOM[$CUSTOM_LOADER]}"
      SHUTDOWN_PORT=25100
      portInUse=$(ps -eaf | grep [b]ulkIngestMap | grep $SHUTDOWN_PORT)
      while [[ ! -z $portInUse ]]; do
        SHUTDOWN_PORT=$((SHUTDOWN_PORT + 1))
        portInUse=$(ps -eaf | grep [b]ulkIngestMap | grep $SHUTDOWN_PORT)
      done
      ${MAP_LOADER_CUSTOM[$CUSTOM_LOADER]} -shutdownPort ${SHUTDOWN_PORT} >>$LOG_DIR/map-file-loader-custom.$CUSTOM_LOADER.log 2>&1 &
    done
  fi
else
        echo "$COUNT map file loaders already running"
fi

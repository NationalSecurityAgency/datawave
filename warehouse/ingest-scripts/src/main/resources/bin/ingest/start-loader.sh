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
PIDS=`$MAPFILE_LOADER_COMMAND_PREFIX pgrep -f "\-Dapp=bulkIngestMapFileLoader"`
COUNT=0
for PID in $PIDS; do
	COUNT=$((COUNT + 1))
done
if [[ COUNT -eq 0 ]]; then
	for (( LOADER=0; LOADER < ${#MAP_LOADER_HDFS_NAME_NODES[@]}; LOADER=$((LOADER + 1)) )); do
                FILES_STUCK_LOADING=`$INGEST_HADOOP_HOME/bin/hadoop fs -ls "${MAP_LOADER_HDFS_NAME_NODES[$LOADER]}$BASE_WORK_DIR/*/job.loading" | awk '{print $NF}'`
                if [[ ! -z $FILES_STUCK_LOADING ]]; then
                    for stuckFile in $FILES_STUCK_LOADING; do
                        echo "Resetting ${stuckFile}"
                        moving_result=`$INGEST_HADOOP_HOME/bin/hadoop fs -mv $stuckFile ${stuckFile%.loading}.complete 2>&1`
                            if [[ ! -z $moving_result ]]; then
                                echo "Error resetting file: $moving_result . Manually check for orphans."
                            fi
                    done
                fi
		COUNT=${NUM_MAP_LOADERS[$LOADER]}
        	echo "starting $COUNT map file loaders for ${MAP_LOADER_HDFS_NAME_NODES[$LOADER]} ..."
		for (( ; $COUNT; COUNT=$((COUNT-1)) )) ; do
        		$MAPFILE_LOADER_CMD -srcHdfs ${MAP_LOADER_HDFS_NAME_NODES[$LOADER]} -destHdfs ${MAP_LOADER_HDFS_NAME_NODES[$LOADER]} -shutdownPort "241$LOADER$COUNT" >> $LOG_DIR/map-file-loader.$LOADER$COUNT.log 2>&1 &
		done
	done
else
        echo "$COUNT map file loaders already running"
fi

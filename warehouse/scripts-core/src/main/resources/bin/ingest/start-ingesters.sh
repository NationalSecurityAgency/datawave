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
    rm -f ${LOCK_FILE_DIR}/INGEST_STARTUP.LCK
    if [[ "$MAP_FILE_LOADER_SEPARATE_START" != "true" ]]; then
	rm -f ${LOCK_FILE_DIR}/LOADER_STARTUP.LCK
    fi
    $0 ${@/-force/}
    exit $?
fi
if [ -e ${LOCK_FILE_DIR}/INGEST_STARTUP.LCK ]; then
    echo "Startup has been locked out.  Use -force to unlock."
    exit -1
fi

if [[ "$@" =~ ".*-skipCache.*" || "$@" =~ "-skipCache" ]]; then
    echo "Skipping job cache check and load"
else
    ../ingest/check-job-cache.sh
    if [[ $? != 0 ]]; then
        echo "Job cache does not appear to be consistent.  Recreating..."
        ../ingest/load-job-cache.sh
    fi
fi

echo "LOG_DIR:" $LOG_DIR
echo "FLAG_DIR:" $FLAG_DIR

CLEAN_CMD=$THIS_DIR/start-cleaner.sh
MAPFILE_LOADER_CMD=$THIS_DIR/start-loader.sh
FLAG_MAKER_CMD=$THIS_DIR/start-flag-maker.sh

PID=`ps -wwef | egrep "bash .*one-hr-ingest-server.sh" | grep -v  grep | awk {'print $2'}`
if [ -z $PID ]; then
        echo "starting one hour ingest server"
        ./one-hr-ingest-server.sh $PWD $FLAG_DIR $LOG_DIR> $LOG_DIR/one-hr-ingest.log 2>&1 &
else
        echo "one hour ingest server already running"
fi

PID=`ps -wwef | egrep "bash .*fifteen-min-ingest-server.sh" | grep -v  grep | awk {'print $2'}`
if [ -z $PID ]; then
        echo "starting fifteen minute ingest server"
        ./fifteen-min-ingest-server.sh $PWD $FLAG_DIR $LOG_DIR> $LOG_DIR/fifteen-min-ingest.log 2>&1 &
else
        echo "fifteen minute ingest server already running"
fi

PID=`ps -wwef | egrep "bash .*five-min-ingest-server.sh" | grep -v  grep | awk {'print $2'}`
if [ -z $PID ]; then
        echo "starting five minute ingest server"
        ./five-min-ingest-server.sh $PWD $FLAG_DIR $LOG_DIR> $LOG_DIR/five-min-ingest.log 2>&1 &
else
        echo "five minute ingest server already running"
fi

sleep 1

PID=`ps -wwef | egrep "python .*cleanupserver.py" | grep -v grep | awk {'print $2'}`
if [ -z $PID ]; then
        echo "starting cleanup server ..."
        $CLEAN_CMD &
else
        echo "cleanup server already running"
fi

if [[ "$MAP_FILE_LOADER_SEPARATE_START" != "true" ]]; then
    echo "starting map file loaders ..."
    $MAPFILE_LOADER_CMD
fi

echo "starting flag makers ..."
$FLAG_MAKER_CMD

echo "done"

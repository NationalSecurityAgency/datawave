#!/bin/bash

. ./common-start.sh

# If the paused file exists, then prevent startup
if [ -e ${LOCK_FILE_DIR}/INGEST_STARTUP.LCK ]; then
    echo "Startup has been locked out.  Use start-ingesters -force to unlock."
    exit -1
fi


$PYTHON $THIS_DIR/cleanup-server.py > $LOG_DIR/cleanup.log 2>&1 < /dev/null &

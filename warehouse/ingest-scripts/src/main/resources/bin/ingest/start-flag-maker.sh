#!/bin/bash

. ./common-start.sh

# If the paused file exists, then prevent startup
if [ -e ${LOCK_FILE_DIR}/INGEST_STARTUP.LCK ]; then
    echo "Startup has been locked out.  Use start-ingesters -force to unlock."
    exit -1
fi

#NEW FLAG MAKER
$THIS_DIR/flag-maker.sh start


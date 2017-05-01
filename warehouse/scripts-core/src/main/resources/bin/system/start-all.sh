#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

. ../ingest/ingest-env.sh

CRON=""
if [[ "$@" =~ ".*-cron.*" || "$@" =~ "-cron" ]]; then
   CRON="-cron";
else
	if [[ ! "$@" =~ ".*-force.*" && ! "$@" =~ "-force" ]]; then
            rm -f ${LOCK_FILE_DIR}/ALL_STARTUP.LCK
            $0 -force
            exit $?
        fi

fi

echo "Starting ingesters..."
./start-ingest.sh $CRON

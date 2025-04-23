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

# If the paused file exists, then prevent startup
if [ -e ${LOCK_FILE_DIR}/INGEST_STARTUP.LCK ]; then
    echo "Startup has been locked out.  Use start-ingesters -force to unlock."
    exit -1
fi

#NEW FLAG MAKER
$THIS_DIR/flag-maker.sh start


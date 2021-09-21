#!/bin/bash

if [[ "$USER" != "datawave" ]]; then
  echo "This script should only be run by the datawave user.  Aborting."
  exit 1
fi

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

#stop scripts do not require force despite lock files
. ../ingest/ingest-env.sh -force

# create the lock file
stop_all

echo "Stopping ingesters..."
./stop-ingest.sh $@

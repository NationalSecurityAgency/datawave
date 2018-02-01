#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
	THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
	THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

#stop scripts do not require force despite lock files
. ../ingest/ingest-env.sh -force

if [[ -z ${LOCK_FILE_DIR} ]]; then
  echo "LOCK_FILE_DIR is not set, be sure to source bin/ingest/ingest-env.sh"
  exit -1
fi

# lock out any startups
touch ${LOCK_FILE_DIR}/METRICS_INGEST_STARTUP.LCK


# pull out the signal if supplied
SIGNAL=""
for arg in $@; do
  if [[ "$arg" == "-force" ]]; then
    SIGNAL="-9"
  elif [[ "$arg" =~ "-[0-9]+" ]]; then
    SIGNAL="$arg"
  elif [[ "$arg" =~ "-SIG[[:graph:]]+" ]]; then
    SIGNAL="$arg"
  fi
done

pkill $SIGNAL -f "\-Dapp=ingestMetricsIngest"
pkill $SIGNAL -f "\-Dapp=loaderMetricsIngest"

# for those systems running metrics ingest under a different user:
$MAP_FILE_LOADER_COMMAND_PREFIX pkill $SIGNAL -f "\-Dapp=ingestMetricsIngest"
$MAP_FILE_LOADER_COMMAND_PREFIX pkill $SIGNAL -f "\-Dapp=loaderMetricsIngest"

#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
	THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
	THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

if [[ $# -eq 0 ]]; then

        echo "Usage: $0 input_dir datatype work_dir [-clientId <id>] [-z] [-force]"
        exit 1
fi

. ../ingest/ingest-env.sh

if [[ -z ${LOCK_FILE_DIR} ]]; then
  echo "LOCK_FILE_DIR is not set, be sure to source bin/ingest/ingest-env.sh"
  exit -1
fi

DIR=$1
TYPE=$2
WORKDIR=$3
EXTRA_OPTS=${@:4}

# If the paused file exists, then prevent startup unless forcing
if [[ "$@" =~ ".*-force.*" || "$@" =~ "-force" ]]; then
    rm -f ${LOCK_FILE_DIR}/POLLER_${TYPE}_STARTUP.LCK
    $0 ${@/-force/}
    exit $?
fi
if [ -e ${LOCK_FILE_DIR}/POLLER_${TYPE}_STARTUP.LCK ]; then
    echo "Startup has been locked out.  Use -force to unlock."
    exit -1
fi

DATE=`date +%Y%m%d`

#Make logs directory if not exists
if [[ ! -d $LOG_DIR ]]; then
  mkdir $LOG_DIR
fi

if ((`pgrep -f "\-Dapp=datawavePoller \-Ddatatype=$TYPE \-DinDir=$DIR \-DoutDir=$WORKDIR" | wc -l`==0))
then
  nohup ./EventPoller.sh $DIR $TYPE $WORKDIR $EXTRA_OPTS >> $LOG_DIR/${DATE}_${TYPE}.log 2>&1 &
else
  echo "Poller already running for type $TYPE" >> $LOG_DIR/${DATE}_${TYPE}.log
fi

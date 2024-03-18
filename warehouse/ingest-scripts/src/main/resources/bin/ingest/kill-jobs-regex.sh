#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

. ingest-env.sh
. findJars.sh
REGEX=$1
TRACKER=$2

ACC_HOME=$INGEST_ACCUMULO_HOME

if [ -z "$TRACKER" ] ;  then
	ACC_HOME=${INGEST_ACCUMULO_HOME}
else
	if [ $TRACKER == "WAREHOUSE" ] ;	then
		ACC_HOME=${WAREHOUSE_ACCUMULO_HOME}
	fi
fi

ADDJARS=$DATAWAVE_INGEST_CORE_JAR

CLASSPATH=$ADDJARS $ACC_HOME/bin/accumulo datawave.ingest.util.KillJobByRegex $REGEX
RETURN_CODE=$?

exit $RETURN_CODE

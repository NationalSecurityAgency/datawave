#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
	THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
	THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

. ingest-env.sh

REGEX=$1
TRACKER=$2
#
# Jars
#
DATAWAVE_INGEST_JAR=$THIS_DIR/../../lib/datawave-ingest-core-$INGEST_VERSION.jar

ACC_HOME=$INGEST_ACCUMULO_HOME

if [ -z "$TRACKER" ] ;  then
	ACC_HOME=${INGEST_ACCUMULO_HOME}
else
	if [ $TRACKER == "WAREHOUSE" ] ;	then
		ACC_HOME=${WAREHOUSE_ACCUMULO_HOME}
	fi
fi


$ACC_HOME/bin/accumulo -add $DATAWAVE_INGEST_JAR nsa.datawave.ingest.util.KillJobByRegex $REGEX
RETURN_CODE=$?

exit $RETURN_CODE

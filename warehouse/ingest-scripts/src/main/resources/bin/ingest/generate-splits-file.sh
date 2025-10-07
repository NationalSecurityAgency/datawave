#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

. ./ingest-libs.sh

export ACCUMULO_OTHER_OPTS="${ACCUMULO_OTHER_OPTS} $HADOOP_INGEST_OPTS"

export CLASSPATH
$WAREHOUSE_ACCUMULO_HOME/bin/accumulo \
  datawave.ingest.util.GenerateSplitsFile \
  -u $USERNAME \
  -p $PASSWORD \
  -i $WAREHOUSE_INSTANCE_NAME \
  -zk $WAREHOUSE_ZOOKEEPERS \
  -cd $(readlink -f $CONF_DIR)

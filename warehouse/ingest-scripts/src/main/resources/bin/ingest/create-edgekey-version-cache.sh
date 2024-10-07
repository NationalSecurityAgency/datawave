#!/bin/bash

function usage
{
        echo -e  "usage: create-edgekey-version-cache.sh [options] where options include:\n
        \t--init\t create a new edge version key in datawave metadata table\n
        \t\t--version is required with this\n
        \t\t*WARNING* only run this once per edge key version number\n
        \t--version\t specify a number for this version
        \t--update\t  will update the edge key version cache file reading the latest version info from the datawave metadata table\n
        \t\t specify the hdfs directory where the cache file will be stored
        \t--help\t print this message\n"
}

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

RUN_OPS=""

#Parse the command line args
while [ "$1" != "" ]; do
        case $1 in
                --init)         RUN_OPS="$RUN_OPS -init"
                                ;;
                --update)       shift
                                RUN_OPS="$RUN_OPS -update $1"
                                ;;
                --version)      shift
                                RUN_OPS="$RUN_OPS -version $1"
                                ;;
                -h | --help)    usage
                                exit
                                ;;
                *)              usage
                                exit
                                ;;
        esac
        shift
done


#
# Get the ingest envirionment variables
#
. ./ingest-env.sh
. ./ingest-libs.sh

ADDJARS=$THIS_DIR/$DATAWAVE_INGEST_CORE_JAR:$THIS_DIR/$COMMON_UTIL_JAR:$THIS_DIR/$DATAWAVE_CORE_JAR:$THIS_DIR/$DATAWAVE_TYPE_UTILS_JAR:$THIS_DIR/$DATAWAVE_COMMON_UTILS_JAR:$THIS_DIR/$COMMONS_LANG_JAR

CLASSPATH=$ADDJARS $WAREHOUSE_ACCUMULO_BIN/accumulo datawave.ingest.util.GenerateEdgeKeyVersionCache -Dfs.default.name=file:/// $RUN_OPS $USERNAME $PASSWORD ${METADATA_TABLE_NAME} $WAREHOUSE_INSTANCE_NAME $WAREHOUSE_ZOOKEEPERS

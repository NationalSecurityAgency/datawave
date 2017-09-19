#!/bin/bash
if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

. ../ingest/ingest-env.sh


if [ $# -le 1 ]
then
        echo "Usage: $0 <hdfsNameNode> <failedWorkDir>"
        echo "    hdfsNameNode = the namenode: e.g. hdfs://warehouse-a"
        echo "    failedWorkDir = the bulk ingest failed dir: e.g. /data/BulkIngest/20111128180632-29241"
        echo "    This utility will recreate the flag file for this job.  First it looks for a copy of"
        echo "    of the flag file in the bulk load directory.  If that does not exist, then it will look for"
        echo "    an archived log from which to recreate the flag file"
        exit 1
fi
HDFS_NAME_NODE=$1
FAILED_WORK_DIR=$2


flagFile=`$WAREHOUSE_HADOOP_HOME/bin/hadoop fs -fs $HDFS_NAME_NODE -ls $FAILED_WORK_DIR/*.flag | grep flag | awk '{print $8}'`
if [[ "$flagFile" != "" ]]; then
  flagFile=${flagFile##*/}
  echo "Recreating flag file using $FAILED_WORK_DIR/$flagFile"
  $WAREHOUSE_HADOOP_HOME/bin/hadoop fs -fs $HDFS_NAME_NODE -copyToLocal $FAILED_WORK_DIR/$flagFile $FLAG_DIR/$flagFile.recreating
  mv $FLAG_DIR/$flagFile.recreating $FLAG_DIR/$flagFile

  echo "Cleaning up $FAILED_WORK_DIR"
  $WAREHOUSE_HADOOP_HOME/bin/hadoop fs -fs $HDFS_NAME_NODE -mv $FAILED_WORK_DIR/job.paths ${FAILED_WORK_DIR}.paths
  $WAREHOUSE_HADOOP_HOME/bin/hadoop fs -fs $HDFS_NAME_NODE -rm -r $FAILED_WORK_DIR
else
  echo "Did not find a copy of the flag file in $FAILED_WORK_DIR, attempting to find archived log"
  date=`$WAREHOUSE_HADOOP_HOME/bin/hadoop fs -fs $HDFS_NAME_NODE -ls $FAILED_WORK_DIR | grep $FAILED_WORK_DIR | awk '{print $6}' | sort | uniq | head -1`
  if [[ "$date" == "" ]]; then
    echo "Could not find specified bulk directory: $FAILED_WORK_DIR"
    exit 1
  fi
  date=`echo $date | sed 's/-//g'`
  LOG=`zgrep -l $FAILED_WORK_DIR $LOG_DIR/archive/$date/*.flag.log.gz`
  if [[ "$LOG" == "" ]]; then
    echo "Could not find log file corresponding to $FAILED_WORK_DIR in $LOG_DIR/archive/$date"
    exit 1
  fi
  echo "Found references to $FAILED_WORK_DIR in $LOG"

  FLAG_FILE=$FLAG_DIR/${LOG##*/}
  FLAG_FILE=${FLAG_FILE%.log.gz}
  echo Creating $FLAG_FILE
  CMD=`zcat $LOG | grep 'Executed Command:' | tail -1 | sed 's/Executed Command: //'`
  if [[ $CMD =~ "-inputFileListMarker" ]]; then
    echo $CMD > $FLAG_FILE
    MARKER=${CMD##*-inputFileListMarker }
    MARKER=${MARKER%% *}
    echo $MARKER >> $FLAG_FILE
    zcat $LOG | grep 'inputPathList is' | tail -1 | sed 's/.*\[//' | sed 's/\].*//' | sed 's/,//g' | tr ' ' '\n' >> $FLAG_FILE
  else
    echo $CMD > $FLAG_FILE
  fi

  $WAREHOUSE_HADOOP_HOME/bin/hadoop fs -fs $HDFS_NAME_NODE -mv $FAILED_WORK_DIR/job.paths ${FAILED_WORK_DIR}.paths
  $WAREHOUSE_HADOOP_HOME/bin/hadoop fs -fs $HDFS_NAME_NODE -rm -r $FAILED_WORK_DIR
fi

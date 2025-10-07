#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

FORCE=true
. ../ingest/ingest-env.sh


params="-destHdfs -majcThreshold"
host=$(hostname -s)
for pid in $($MAPFILE_LOADER_COMMAND_PREFIX pgrep -f "\-Dapp=bulkIngestMapFileLoader"); do
   process=`ps -ef | grep $pid | grep -v grep`
   detail=""
   for param in $params; do
     txt=`echo $process | sed "s/.*\($param [^ ]*\).*/\1/"`
     if [[ -z $detail ]]; then
       detail="$txt"
     else
       detail="$detail $txt"
     fi
   done
   echo "$host: $pid: Map File Loader $detail"
done

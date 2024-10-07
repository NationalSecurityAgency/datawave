#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
    THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
    MKTEMP="mktemp -t $(basename $0)"
else
    THIS_SCRIPT=$(readlink -f $0)
    MKTEMP="mktemp -t $(basename $0).XXXXXXXX"
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

FORCE=true
. ./ingest-env.sh


file=$($MKTEMP)
for (( i=33; i < 127; i=i+1 )); do
    hexint=$(printf %x $i)
    printf "\x$hexint\n" >> $file
done

script=$($MKTEMP)
echo "addsplits -t ${SHARD_INDEX_TABLE_NAME} -sf $file
addsplits -t ${SHARD_STATS_TABLE_NAME} -sf $file
addsplits -t ${SHARD_REVERSE_INDEX_TABLE_NAME} -sf $file
quit" > $script

$WAREHOUSE_ACCUMULO_BIN/accumulo shell -u $USERNAME -p $PASSWORD -f $script

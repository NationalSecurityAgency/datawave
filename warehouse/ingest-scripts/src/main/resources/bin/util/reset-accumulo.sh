#!/bin/bash

#*************************************************************************************
#*************************************************************************************
#
# This script is intended to simplify the process of resetting your local Accumulo 
# host for DataWave dev/testing purposes only. Not intended for use within a production 
# environment.
#
# You can and should customize the behavior below to suit your particular needs.
# This script can be used as a companion script for 'util/build-deploy.sh', if desired.
#
#*************************************************************************************
#*************************************************************************************

if [[ `uname` == "Darwin" ]]; then
    THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
    MKTEMP="mktemp -t `basename $0`"
else
    THIS_SCRIPT=`readlink -f $0`
    MKTEMP="mktemp -t `basename $0`.XXXXXXXX"
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

FORCE=true
. ./ingest-env.sh

# Generate 'typical' splits for the fwd & rev index tables and 
# write out to temp file...

file=`$MKTEMP`
for (( i=33; i < 127; i=i+1 )); do
    hexint=`printf %x $i`
    printf "\x$hexint\n" >> $file
done

script=`$MKTEMP`
echo "deleterows -t DatawaveMetadata -f
deleterows -t LoadDates -f
deleterows -t PrincipalCache -f
deleterows -t QueryMetrics_m -f
deleterows -t protobufedge -f
deleterows -t dateIndex -f
deleterows -t errorIndex -f
deleterows -t errorMetadata -f
deleterows -t errorReverseIndex -f
deleterows -t errorShard -f
deleterows -t shard -f
deleterows -t shardIndex -f
deleterows -t shardReverseIndex -f
addsplits 20160426_0 20160426_1 20160426_2 20160426_3 20160426_4 20160426_5 20160426_6 20160426_7 20160426_8 20160426_9 -t shard
addsplits -t shardIndex -sf $file
addsplits -t shardReverseIndex -sf $file
quit" > $script

# Execute the dynamically generated script using Accumulo...

$WAREHOUSE_ACCUMULO_BIN/accumulo shell -u $USERNAME -p $PASSWORD -f $script

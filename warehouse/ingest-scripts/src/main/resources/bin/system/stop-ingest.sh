#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
    THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
    MKTEMP="mktemp -t `basename $0`"
else
    THIS_SCRIPT=`readlink -f $0`
    MKTEMP="mktemp -t `basename $0`.XXXXXXXX"
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR
script_name=$(basename ${0})

#stop scripts do not require force despite lock files
. ../ingest/ingest-env.sh -force
. ../util/logging_pdsh.sh

export INGEST_BIN=$THIS_DIR/..

# now apply the appropriate system configuration
if [[ "$INGEST_HOST" == "localhost" || "$INGEST_HOST" == `hostname` || "$INGEST_HOST" == `hostname -s` ]]; then

  $INGEST_BIN/ingest/stop-ingesters.sh $@

else

  ingestHost=`$MKTEMP`
  trap 'rm -f "$ingestHost"; exit $?' INT TERM EXIT
  echo $INGEST_HOST > $ingestHost

  logging_pdsh "${script_name}" -f 25 -w ^${ingestHost} "$INGEST_BIN/ingest/stop-ingesters.sh $@"

  rm $ingestHost
  trap - INT TERM EXIT

fi

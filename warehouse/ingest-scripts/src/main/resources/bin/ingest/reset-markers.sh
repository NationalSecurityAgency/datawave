#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

# Environment
FORCE=true
. ./ingest-env.sh

for x in $(ls -1 ${FLAG_DIR}/*.marker); do
   mv $x ${x%.marker}
done


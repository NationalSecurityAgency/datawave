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


host=$(hostname -s)
ps -efww | egrep "bash .*bulk-ingest-server.sh" | grep -v grep | sed "s/[[:graph:]]* *\([[:digit:]]*\).*/$host: \1: Bulk Ingest Server/"
ps -efww | egrep "bash .*live-ingest-server.sh" | grep -v grep | sed "s/[[:graph:]]* *\([[:digit:]]*\).*/$host: \1: Live Ingest Server/"
ps -efww | egrep "bash .*execute-ingest.sh.*" | grep -v grep | sed "s/[[:graph:]]* *\([[:digit:]]*\).*ingest.sh .*\/flags\/\([[:graph:]]*\).*/$host: \1: Ingest Job: \2/"
ps -efww | egrep "python .*cleanup-server.py.*" | grep -v grep | sed "s/[[:graph:]]* *\([[:digit:]]*\).*/$host: \1: Cleanup Server/"
./list-loader.sh
ps -efww | egrep "python .*_flag_maker.py.*" | grep -v grep | grep -v sed | sed "s/[[:graph:]]* *\([[:digit:]]*\).*\/ingest\/\(.*\)_flag_maker.py.*/$host: \1: Flag Maker: \2/"
jps -m | egrep FlagMaker | grep -v grep | grep -v sed | sed "s/\([[:digit:]]*\).*-flagConfig \(.*\)/$host: \1: Flag Maker: \2/"

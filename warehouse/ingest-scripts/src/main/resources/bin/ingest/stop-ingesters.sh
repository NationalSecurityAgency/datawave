#! /bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

#stop scripts do not require force despite lock files
. ../ingest/ingest-env.sh -force

if [[ -z ${LOCK_FILE_DIR} ]]; then
  echo "LOCK_FILE_DIR is not set, be sure to source bin/ingest/ingest-env.sh"
  exit -1
fi

# lock out any startups
touch ${LOCK_FILE_DIR}/INGEST_STARTUP.LCK


# pull out the signal if supplied
SIGNAL=""
for arg in $@; do
  if [[ "$arg" == "-force" ]]; then
    SIGNAL="-9"
  elif [[ "$arg" =~ -[0-9]+ ]]; then
    SIGNAL="$arg"
  elif [[ "$arg" =~ -SIG[[:graph:]]+ ]]; then
    SIGNAL="$arg"
  fi
done


STOP_INGEST_SERVERS_CMD=$THIS_DIR/stop-ingest-servers.sh

$STOP_INGEST_SERVERS_CMD -type all -signal $SIGNAL

./kill-jobs-regex.sh '.*IngestJob.*'

for f in $(shopt -s extglob; find ${FLAG_DIR} -regextype posix-egrep -regex ".*\.flag\..*\.marker" 2>/dev/null); do
   flag_file=$(flagBasename $f).flag
   mv $f $flag_file
done

PID=$(ps -wwef | egrep "python .*cleanup-server.py" | grep -v grep | awk {'print $2'})
if [ -z "$PID" ]; then
        echo "no cleanup server running"
else
        echo "stopping cleanup server"
        kill $SIGNAL $PID
fi

if [[ "$MAP_FILE_LOADER_SEPARATE_START" != "true" ]]; then
    $THIS_DIR/stop-loader.sh
fi

$THIS_DIR/flag-maker.sh stop

echo "done"

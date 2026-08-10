#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

. ../util/pid-functions.sh

function usage
{
    echo -e "usage: stop-ingest-servers.sh [options] where options include:\n
    \t-type\tWhich ingesters to stop (all,bulk,live) eg -type all
    \t-signal\tSignal command to send as first arg to kill
    \t-help\tprint this message\n"
}

MAX_SLEEP_TIME_SECS="${MAX_SLEEP_TIME_SECS:-30}"

BULK_SCRIPT="bulk-ingest-server.sh"
BULK_TEXT="bulk"
LIVE_SCRIPT="live-ingest-server.sh"
LIVE_TEXT="live"

TYPE=""

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        -type)
            TYPE="$2"
            shift
        ;;
        -signal)
            SIGNAL="$2"
            shift
        ;;
        -help)
            usage
            exit
        ;;
        *)
            echo -e "Unknown option: $key"
            usage
            exit
        ;;
    esac
    shift
done

TYPES_TO_RUN=""

case $TYPE in
    all)
        TYPES_TO_RUN="BULK LIVE"
    ;;
    bulk)
        TYPES_TO_RUN="BULK"
    ;;
    live)
        TYPES_TO_RUN="LIVE"
    ;;
    *)
        echo "UNKNOWN TYPE -- Was $TYPE but expected all, bulk, or live"
        exit
    ;;
esac

for type in $TYPES_TO_RUN
do
    script=${type}_SCRIPT
    text=${type}_TEXT
    PID=$(ps -wwef | egrep "bash .*${!script}" | grep -v grep | awk {'print $2'})

     if [ -z "$PID" ]; then
        echo "no ${!text} ingest server running"
     else
        echo "stopping ${!text} ingest server $SIGNAL"
        kill $SIGNAL $PID
        pid::waitForDeath ${PID} ${MAX_SLEEP_TIME_SECS}
     fi
done

echo "done stopping ingest servers"

#!/bin/bash
function usage
{
    echo -e "usage: start-ingest-servers.sh [options] where options include:\n
    \t-force\tstart servers ignoring presence of ${LOCK_FILE_DIR}/INGEST_STARTUP.LCK
    \t-type\tWhich ingesters to start (all,bulk,live) eg -type all
    \t-help\tprint this message\n"
}

BULK_SCRIPT="bulk-ingest-server.sh"
BULK_TEXT="bulk"
BULK_LOG="bulk-ingest.log"

LIVE_SCRIPT="live-ingest-server.sh"
LIVE_TEXT="live"
LIVE_LOG="live-ingest.log"

TYPE=""

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        -type)
            TYPE="$2"
            shift
        ;;
        -force)
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
    log=${type}_LOG
    text=${type}_TEXT
    PID=$(ps -wwef | egrep "bash .*${!script}" | grep -v grep | awk {'print $2'})

     if [ -z "$PID" ]; then
        echo "starting ${!text} ingest server"
        ./${!script} $PWD $FLAG_DIR $LOG_DIR> $LOG_DIR/${!log} 2>&1 &
        echo "started ${!text} ingest server"
     else
        echo "${!text} ingest server already running"
     fi
done

echo "done starting ingest servers"

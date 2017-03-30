#!/bin/bash
function usage
{
    echo -e "usage: start-ingest-servers.sh [options] where options include:\n
    \t-force\tstart servers ignoring presence of ${LOCK_FILE_DIR}/INGEST_STARTUP.LCK
    \t-type\tWhich ingesters to start (all,onehr,fivemin,fifteenmin) eg -type all
    \t-help\tprint this message\n"
}

ONEHR_SCRIPT="one-hr-ingest-server.sh"
ONEHR_TEXT="one hour"
ONEHR_LOG="one-hr-ingest.log"
FIFTEENMIN_SCRIPT="fifteen-min-ingest-server.sh"
FIFTEENMIN_TEXT="fifteen minute"
FIFTEENMIN_LOG="fifteen-min-ingest.log"
FIVEMIN_SCRIPT="five-min-ingest-server.sh"
FIVEMIN_TEXT="five minute"
FIVEMIN_LOG="five-min-ingest.log"

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
        TYPES_TO_RUN="ONEHR FIFTEENMIN FIVEMIN"
    ;;
    onehr)
        TYPES_TO_RUN="ONEHR"
    ;;
    fifteenmin)
        TYPES_TO_RUN="FIFTEENMIN"
    ;;
    fivemin)
        TYPES_TO_RUN="FIVEMIN"
    ;;
    *)
        echo "UNKNOWN TYPE -- Was $TYPE but expected all, onehr, fifteenmin, or fivemin"
        exit
    ;;
esac

for type in $TYPES_TO_RUN
do
    script=${type}_SCRIPT
    log=${type}_LOG
    text=${type}_TEXT
    PID=`ps -wwef | egrep "bash .*${!script}" | grep -v grep | awk {'print $2'}`

     if [ -z "$PID" ]; then
        echo "starting ${!text} ingest server"
        ./${!script} $PWD $FLAG_DIR $LOG_DIR> $LOG_DIR/${!log} 2>&1 &
        echo "started ${!text} ingest server"
     else
        echo "${!text} ingest server already running"
     fi
done

echo "done starting ingest servers"

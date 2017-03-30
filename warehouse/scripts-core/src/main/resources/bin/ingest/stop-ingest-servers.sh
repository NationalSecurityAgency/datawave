#!/bin/bash
function usage
{
    echo -e "usage: stop-ingest-servers.sh [options] where options include:\n
    \t-type\tWhich ingesters to stop (all,onehr,fivemin,fifteenmin) eg -type all
    \t-signal\tSignal command to send as first arg to kill
    \t-help\tprint this message\n"
}

ONEHR_SCRIPT="one-hr-ingest-server.sh"
ONEHR_TEXT="one hour"
FIFTEENMIN_SCRIPT="fifteen-min-ingest-server.sh"
FIFTEENMIN_TEXT="fifteen minute"
FIVEMIN_SCRIPT="five-min-ingest-server.sh"
FIVEMIN_TEXT="five minute"

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
    text=${type}_TEXT
    PID=`ps -wwef | egrep "bash .*${!script}" | grep -v grep | awk {'print $2'}`

     if [ -z "$PID" ]; then
        echo "no ${!text} ingest server running"
     else
        echo "stopping ${!text} ingest server $SIGNAL"
        kill $SIGNAL $PID
     fi
done

echo "done stopping ingest servers"

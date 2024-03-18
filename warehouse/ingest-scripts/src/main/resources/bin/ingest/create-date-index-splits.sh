#!/bin/bash

# Syntax: create_dateIndex_splits.sh [-d yyyyMMdd] [-n N]
#   -d yyyyMMdd specifies the date to create a split for.  Default is tomorrow.
#   -n N specifies the number of days for which to create splits.  Default is 1.

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

. ./ingest-libs.sh

# Default date is tomorrow
DATE=$(date -d tomorrow +%Y%m%d)

# Default is one split
NUM_DAYS=1

# optional argument -d YYYYMMDD
while getopts d:n: FLAG; do
  case $FLAG in
    d) DATE=$(echo $OPTARG | egrep '^[0-9]{8}$')
      if [ "$DATE" == "" ]; then
        echo "DATE must be in YYYYMMDD format, not $OPTARG"
        exit 1
      fi
      date -d $DATE 2>$1 > /dev/null
      if [ "$?"  != "0" ]; then
        echo "Not a valid date: $DATE"
        exit $?
      fi
    ;;
    n) NUM_DAYS=$(echo $OPTARG | egrep '^[0-9]+$')
      if [ "$NUM_DAYS" == "" ]; then
        echo "NUM_DAYS must be a number, not $OPTARG"
        exit 1
      fi
    ;;
    *) usage
       exit
    ;;
  esac
done

DAILY_SPLITS=$(for ((i = 0; i < $NUM_DAYS; i++)); do for ((j = 0; j < $NUM_DATE_INDEX_SHARDS; j++)); do date -d "${DATE} + ${i} days" +%Y%m%d | awk "{print \\$1 \"_\" ${j}}" ; done ; done | tr "\n" " ")
echo "Creating the following splits for dateIndex: $DAILY_SPLITS"
$WAREHOUSE_ACCUMULO_HOME/bin/accumulo shell -u $USERNAME -p $PASSWORD -e "addsplits ${DAILY_SPLITS} -t dateIndex" -zi $WAREHOUSE_INSTANCE_NAME -zh $WAREHOUSE_ZOOKEEPERS

#!/bin/bash

LOG_DIR=${jboss.log.dir}
WORK_DIR=${LOG_DIR}/cached_results_exports

TABLE_DIRS=${WORK_DIR}/cached_results_cleanup_dirs.out
LOCK_FILE=${WORK_DIR}/cached_results_cleanup.lock

if [[ ! -d ${WORK_DIR} ]]; then
    mkdir -p ${WORK_DIR}
fi

if [ -e $LOCK_FILE ]; then
    exit 0
fi
touch $LOCK_FILE

(( EXPR_TIME = ${cached_results.daysToLive} * 86400 ))

while [[ 1 -eq 1 ]]; do

        hadoop fs -fs ${cached.results.hdfs.uri} -ls ${cached.results.export.dir}/* | awk '{ print $8"@"$6"_"$7 }' > $TABLE_DIRS

        for line in `cat $TABLE_DIRS`; do
          #How old is the directory
          TABLE_DIR=`echo $line | cut -d@ -f1`
          DIR_DATE=`echo $line | cut -d@ -f2`
          DIR_TIME=`date --date="${DIR_DATE/_/ }" +%s`
          NOW=`date +%s`
          (( DIR_AGE = NOW - DIR_TIME ))

          if (( DIR_AGE > EXPR_TIME )); then
            echo "Removing expired directory ${TABLE_DIR}"
            hadoop fs -fs ${cached.results.hdfs.uri} -rm -R ${TABLE_DIR}
          fi

        done

        echo "Sleeping..."
        sleep 30

done

rm $LOCK_FILE

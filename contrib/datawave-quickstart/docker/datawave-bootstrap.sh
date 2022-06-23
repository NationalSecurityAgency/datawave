#!/usr/bin/env bash

source ~/.bashrc

# Update Accumulo bind host if it's not set to localhost
if [ "${DW_ACCUMULO_BIND_HOST}" != "localhost" ] ; then
   sed -i'' -e "s/localhost/${DW_ACCUMULO_BIND_HOST}/g" ${ACCUMULO_HOME}/conf/gc
   sed -i'' -e "s/localhost/${DW_ACCUMULO_BIND_HOST}/g" ${ACCUMULO_HOME}/conf/masters
   sed -i'' -e "s/localhost/${DW_ACCUMULO_BIND_HOST}/g" ${ACCUMULO_HOME}/conf/monitor
   sed -i'' -e "s/localhost/${DW_ACCUMULO_BIND_HOST}/g" ${ACCUMULO_HOME}/conf/slaves
   sed -i'' -e "s/localhost/${DW_ACCUMULO_BIND_HOST}/g" ${ACCUMULO_HOME}/conf/tracers
fi

START_AS_DAEMON=true

START_ACCUMULO=false
START_WEB=false
START_TEST=false
START_INGEST=false

for arg in "$@"
do
    case "$arg" in
          --bash)
             START_AS_DAEMON=false
             ;;
          --accumulo)
             START_ACCUMULO=true
             ;;
          --web)
             START_WEB=true
             ;;
          --test)
             START_TEST=true
             START_AS_DAEMON=false
             ;;
          --ingest)
             START_INGEST=true
             ;;
          *)
             echo "Invalid argument passed to $( basename "$0" ): $arg"
             exit 1
    esac
done

[ "${START_INGEST}" == true ] && datawaveIngestStart

[ "${START_ACCUMULO}" == true ] && accumuloStart

[ "${START_WEB}" == true ] && datawaveWebStart

if [ "${START_TEST}" == true ] ; then
    datawaveWebStart && datawaveWebTest --blacklist-files QueryMetrics && allStop
    exit $?
fi

if [ "${START_AS_DAEMON}" == true ] ; then
    while true; do sleep 1000; done
fi

exec /bin/bash

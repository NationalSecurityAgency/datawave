#!/usr/bin/env bash

source ~/.bashrc

START_AS_DAEMON=true

START_WEB=false
START_INGEST=false

for arg in "$@"
do
    case "$arg" in
          --bash)
             START_AS_DAEMON=false
             ;;
          --web)
             START_WEB=true
             ;;
          --webdebug)
             START_WEB_DEBUG=true
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

[ "${START_WEB}" == true ] && datawaveWebStart

[ "${START_WEB_DEBUG}" == true ] && datawaveWebStart --debug

if [ "${START_TEST}" == true ] ; then
    datawaveWebStart
    status=$?

    if [ "$status" != "0" ] ; then
        echo "datawaveWebStart Failed"
        cat ${WILDFLY_HOME}/standalone/log/server.log
        exit $status
    else
        datawaveWebTest --blacklist-files QueryMetrics
        status=$?

        if [ "$status" != "0" ] ; then
            echo "datawaveWebTest Failed"
            cat ${WILDFLY_HOME}/standalone/log/server.log
        fi

        allStop
        exit $status
    fi
fi

if [ "${START_AS_DAEMON}" == true ] ; then
    while true; do sleep 1000; done
fi

exec /bin/bash

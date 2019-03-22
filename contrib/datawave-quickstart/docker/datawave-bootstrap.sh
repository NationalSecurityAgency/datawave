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

if [ "${START_AS_DAEMON}" == true ] ; then
    while true; do sleep 1000; done
fi

exec /bin/bash

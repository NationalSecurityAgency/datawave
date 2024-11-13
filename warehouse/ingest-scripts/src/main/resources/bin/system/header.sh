#!/bin/bash

#########################################################
### Default header inclusion that enforces lock files ###
### to allow us to never disable the crontab          ###
#########################################################

if [[ -z ${LOCK_FILE_DIR} ]]; then
  echo "LOCK_FILE_DIR is not set, be sure to source bin/ingest/ingest-env.sh"
  exit -1
fi

LAST_DIR=`pwd`
cd $LAST_DIR
# If the paused file exists, then prevent startup unless forcing
if [[ "$@" =~ ".*-allforce.*" || "$@" =~ "-allforce" ]]; then
    rm -f ${LOCK_FILE_DIR}/ALL_STARTUP.LCK
    $0 ${@/-allforce/} -force
    exit $?
fi

if [[ "$@" =~ ".*-force.*" || "$@" =~ "-force" || "$FORCE" == "true" ]]; then 
	export STARTING=true
fi

if [ -e ${LOCK_FILE_DIR}/ALL_STARTUP.LCK ]; then
	if [ -z "$STOPPING" ]; then
		if [ -z "$STARTING" ]; then
		    echo "All system services are currently stopped.  Use -force to restart individual services, and -allforce to restart all services."
		    exit -1
		fi
	fi
fi


#calling stop all identifies that we are stopping
#everything, therefore, we set an environment variable to identify
#this so that other stop scripts don't fail immediately from the existence
# of ALL_STARTUP_LCK
stop_all(){
	echo `date +%s` > ${LOCK_FILE_DIR}/ALL_STARTUP.LCK
	export STOPPING=true
}

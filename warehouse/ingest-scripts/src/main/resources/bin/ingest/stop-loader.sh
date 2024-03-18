#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

#stop scripts do not require force despite lock files
. ../ingest/ingest-env.sh -force

if [[ -z ${LOCK_FILE_DIR} ]]; then
  echo "LOCK_FILE_DIR is not set, be sure to source bin/ingest/ingest-env.sh"
  exit -1
fi

# lock out any startups
touch ${LOCK_FILE_DIR}/LOADER_STARTUP.LCK

# pull out the signal if supplied
SIGNAL=""
for arg in $@; do
  if [[ "$arg" == "-force" ]]; then
    SIGNAL="-9"
  elif [[ "$arg" =~ -[0-9]+ ]]; then
    SIGNAL="$arg"
  elif [[ "$arg" =~ -SIG[[:graph:]]+ ]]; then
    SIGNAL="$arg"
  fi
done

PIDS=$($MAP_FILE_LOADER_COMMAND_PREFIX pgrep -f "\-Dapp=bulkIngestMapFileLoader")
COUNT=0
for PID in $PIDS; do
        COUNT=$((COUNT + 1))
done
if [[ COUNT -eq 0 ]]; then
        echo "no map file loader running"
else
        echo "stopping $COUNT map file loaders"
	COUNT=0
	WAIT=0
	for PID in $PIDS; do
		if [[ "$SIGNAL" == "" ]]; then
			WAIT=1
			PORT=$(ps -wwfp $PID | grep bulkIngestMapFileLoader | sed 's/.*-DshutdownPort=\([[:graph:]]*\).*/\1/')
			echo "Sending stop command to map file loader $PID using shutdown port $PORT"
			echo "quit" | curl "telnet://localhost:$PORT"
		else
			$MAP_FILE_LOADER_COMMAND_PREFIX kill $SIGNAL $PID
		fi
	done
	
	# Wait in a loop until the BulkIngestMapFileLoader processes have completed (or the user interrupts)
	if ((WAIT)); then
		trap "echo -ne '\nWARNING: Aborted waiting for map file loaders.'; break" INT
		echo -n "Waiting for map file loaders to exit (Press CTRL-C to skip waiting)."
		sleep 1
		PIDS=$($MAP_FILE_LOADER_COMMAND_PREFIX pgrep -f "\-Dapp=bulkIngestMapFileLoader")
		until [ -z "$PIDS" ]; do
			echo -n "."
			sleep 15
			PIDS=$($MAP_FILE_LOADER_COMMAND_PREFIX pgrep -f "\-Dapp=bulkIngestMapFileLoader")
		done
		echo -e "\nAll map file loaders have exited."
		trap - INT
	fi
fi

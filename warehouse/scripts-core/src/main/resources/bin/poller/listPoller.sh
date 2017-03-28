#!/bin/bash
FORCE=true
host=`hostname -s`
pids=`pgrep -f "\-Dapp=datawavePoller"`
if [[ "$pids" != "" ]]; then
   ps -wwfp $pids | grep datawavePoller | sed "s/[[:graph:]]* *\([[:digit:]]*\).* -Ddatatype=\([[:graph:]]*\).* -DinDir=\([[:graph:]]*\).*/$host: \1: Poller \2 from \3/"
fi


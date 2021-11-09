#! /bin/bash
if [[ `uname` == "Darwin" ]]; then
	THIS_SCRIPT=`python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0`
else
	THIS_SCRIPT=`readlink -f $0`
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

#stop scripts do not require force despite lock files
. ../ingest/ingest-env.sh -force

if [[ -z ${LOCK_FILE_DIR} ]]; then
  echo "LOCK_FILE_DIR is not set, be sure to source bin/ingest/ingest-env.sh"
  exit -1
fi

# lock out any startups
touch ${LOCK_FILE_DIR}/INGEST_STARTUP.LCK

INGEST_JOB_POSTKILL_DELAY_SECS="${INGEST_JOB_POSTKILL_DELAY_SECS}"
INGEST_JOB_KILL_MAX_RETRIES="${INGEST_JOB_KILL_MAX_RETRIES}"
INGEST_JOB_KILL_LOOP_DELAY_SECS="${INGEST_JOB_KILL_LOOP_DELAY_SECS}"

[[ -n "${INGEST_JOB_POSTKILL_DELAY_SECS}" ]] || { echo "Must define INGEST_JOB_POSTKILL_DELAY_SECS"; exit 1; }
[[ -n "${INGEST_JOB_KILL_MAX_RETRIES}" ]] || { echo "Must define INGEST_JOB_KILL_MAX_RETRIES"; exit 1; }
[[ -n "${INGEST_JOB_KILL_LOOP_DELAY_SECS}" ]] || { echo "Must define INGEST_JOB_KILL_LOOP_DELAY_SECS"; exit 1; }

function getNumRunningJobs() {
  echo "$(jps -lm | grep -c datawave.ingest.mapreduce.job.IngestJob)"
}

function killYarnJobsAndWait() {
  echo "Terminating YARN IngestJobs"
  ./kill-jobs-regex.sh '.*IngestJob.*'

  echo "Waiting ${INGEST_JOB_POSTKILL_DELAY_SECS} seconds for jobs to cleanup"
  # Grace period to let IngestJob's finish cleanup
  sleep "${INGEST_JOB_POSTKILL_DELAY_SECS}"
}

function startYarnKillLoop() {
  killYarnJobsAndWait

  numRunningJobs=$(getNumRunningJobs)
  retryNum=1

  while [[ ${numRunningJobs} > 0 && ${retryNum} < ${INGEST_JOB_KILL_MAX_RETRIES} ]]; do
    echo "Retry ${retryNum}/${INGEST_JOB_KILL_MAX_RETRIES}: ${numRunningJobs} jobs still running"
    killYarnJobsAndWait

    numRunningJobs=$(getNumRunningJobs)

    if [[ ${numRunningJobs} > 0 ]]; then
      echo "${numRunningJobs} jobs still running. Waiting for ${INGEST_JOB_KILL_LOOP_DELAY_SECS} seconds before issuing the next yarn kill."
      sleep "${INGEST_JOB_KILL_LOOP_DELAY_SECS}"
    fi

    numRunningJobs=$(getNumRunningJobs)
    retryNum=$(( retryNum + 1 ))
  done

  numRunningJobs=$(getNumRunningJobs)
  if [[ ${numRunningJobs} > 0 ]]; then
    echo "ERROR: Out of retries but ${numRunningJobs} jobs still running. Manual intervention is needed. Terminate rogue jobs and try again."
    exit 1
  else
    echo "All jobs killed."
  fi
}

# pull out the signal if supplied
SIGNAL=""
for arg in $@; do
  if [[ "$arg" == "-force" ]]; then
    SIGNAL="-9"
  elif [[ "$arg" =~ "-[0-9]+" ]]; then
    SIGNAL="$arg"
  elif [[ "$arg" =~ "-SIG[[:graph:]]+" ]]; then
    SIGNAL="$arg"
  fi
done


STOP_INGEST_SERVERS_CMD=$THIS_DIR/stop-ingest-servers.sh

$STOP_INGEST_SERVERS_CMD -type all -signal $SIGNAL

startYarnKillLoop

for f in `shopt -s extglob; find ${FLAG_DIR} -regextype posix-egrep -regex ".*\.flag\..*\.marker" 2>/dev/null`; do
   flag_file=$(flagBasename $f).flag
   mv $f $flag_file
done

PID=`ps -wwef | egrep "python .*cleanup-server.py" | grep -v grep | awk {'print $2'}`
if [ -z "$PID" ]; then
        echo "no cleanup server running"
else
        echo "stopping cleanup server"
        kill $SIGNAL $PID
fi

if [[ "$MAP_FILE_LOADER_SEPARATE_START" != "true" ]]; then
    $THIS_DIR/stop-loader.sh
fi

$THIS_DIR/flag-maker.sh stop

echo "done"

#!/bin/bash

PID_FUNCTIONS_DEFINED="${PID_FUNCTIONS_DEFINED}"
[[ -n "${PID_FUNCTIONS_DEFINED}" ]] && return # already sourced

#######################################
# Waits for a PID to terminate up to a given threshold
# Arguments:
#  1) The PID to wait for
#  2) The max time (in seconds) to wait
#######################################
function pid::waitForDeath() {
  local _pid=$1
  local _maxTimeToWait=$2

  if [[ -z "${_pid}" ]] || [[ -z "${_maxTimeToWait}" ]]; then
    echo "Expected <pid> <maxTimeToWaitSecs>"
    return 1
  fi

  local _sleepTime=1
  local _totalWaitTime=0

  while (( _totalWaitTime < _maxTimeToWait )) && kill -0 "${_pid}" 2>/dev/null; do
    sleep ${_sleepTime}
    _totalWaitTime=$(( _totalWaitTime + _sleepTime ))
  done

  if (( _totalWaitTime >= _maxTimeToWait )); then
    echo "Exceeded max wait time. Process ${_pid} may still be running."
    return 2
  else
    return 0
  fi
}

PID_FUNCTIONS_DEFINED="true"

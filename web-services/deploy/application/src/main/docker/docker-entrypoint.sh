#!/usr/bin/env bash

wait_on_shutdown=true

disable_shutdown_wait(){
  wait_on_shutdown=false
  kull -USR1 $CMD_PID
}

wait_and_shutdown(){
  if [ $wait_on_shutdown = true ]; then
    if [[ -r /tmp/shutdown_wait_time ]]; then
      TIMEOUT_MINUTES=$(</tmp/shutdown_wait_time)
    else
      TIMEOUT_MINUTES=75
    fi

    echo "Sending wait and shutdown command to the web service. Will wait up to $TIMEOUT_MINUTES minutes for queries to complete."
    curl --fail -s -o /tmp/curl_shutdown.log http://localhost:8080/DataWave/Common/Health/shutdown?timeoutMinutes=$TIMEOUT_MINUTES
    CURL_STATUS=$?
    if [ $CURL_STATUS -ne 0 ]; then
      echo "Curl failed with status $CURL_STATUS. Command output follows. "
      cat /tmp/curl_shutdown.log
      echo "Sending kill"
      kill $CMD_PID
    fi
  else
    kill $CMD_PID
  fi
}


echo "Capturing ENV Properties"
printenv > env.properties
echo "Setting Runtime Config"
$WILDFLY_HOME/bin/jboss-cli.sh --file=./runtime-config.cli --properties=env.properties

if [[ "$@" != *"bin/standalone.sh"* ]]; then
  exec "$@"
else
  trap 'disable_shutdown_wait' USR1
  trap 'wait_and_shutdown' TERM

  trap 'kill -HUP $CMD_PID' HUP
  trap 'kill -INT $CMD_PID' INT
  trap 'kill -QUIT $CMD_PID' QUIT
  trap 'kill -ILL $CMD_PID' ILL
  trap 'kill -TRAP $CMD_PID' TRAP
  trap 'kill -ABRT $CMD_PID' ABRT
  trap 'kill -FPE $CMD_PID' FPE
  trap 'kill -KILL $CMD_PID' KILL
  trap 'kill -BUS $CMD_PID' BUS
  trap 'kill -SEGV $CMD_PID' SEGV
  trap 'kill -SYS $CMD_PID' SYS
  trap 'kill -PIPE $CMD_PID' PIPE
  trap 'kill -ALRM $CMD_PID' ALRM
  trap 'kill -URG $CMD_PID' URG
  trap 'kill -STOP $CMD_PID' STOP
  trap 'kill -TSTP $CMD_PID' TSTP
  trap 'kill -CONT $CMD_PID' CONT
  trap 'kill -CHLD $CMD_PID'  CHLD
  trap 'kill -TTOU $CMD_PID'  TTOU
  trap 'kill -TTIN $CMD_PID'  TTIN
  trap 'kill -IO $CMD_PID'  IO
  trap 'kill -XCPU $CMD_PID'  XCPU
  trap 'kill -XFSZ $CMD_PID'  XFSZ
  trap 'kill -VTALRM $CMD_PID'  VTALRM
  trap 'kill -PROF $CMD_PID'  PROF
  trap 'kill -WINCH $CMD_PID'  WINCH
  trap 'kill -USR2 $CMD_PID'  USR2

  eval "$@" "&"
  CMD_PID=${!}

  wait $CMD_PID 2>/dev/null
fi
#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
  THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
else
  THIS_SCRIPT=$(readlink -f "$0")
fi

THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR || exit

#
# Get the classpath
#
. ../ingest/ingest-libs.sh

#
# Get environment variables (WAREHOUSE_INSTANCE_NAME, WAREHOUSE_ZOOKEEPERS, USERNAME, PASSWORD, etc.)
#
. ../ingest/ingest-env.sh

TIMEOUT="${WAIT_FOR_BALANCE_TIMEOUT:-3600}"

echo "----------------------------------------------------------------------"
echo " Waiting for Accumulo balance"
echo "----------------------------------------------------------------------"
echo " Instance:     ${WAREHOUSE_INSTANCE_NAME}"
echo " Zookeepers:   ${WAREHOUSE_ZOOKEEPERS}"
echo " User:         ${USERNAME}"
echo " Timeout (s):  ${TIMEOUT}"
echo "----------------------------------------------------------------------"

SYS_PROPS=(
  "-Dinstance.name=${WAREHOUSE_INSTANCE_NAME}"
  "-Dinstance.zookeepers=${WAREHOUSE_ZOOKEEPERS}"
  "-Duser=${USERNAME}"
  "-Dpassword=${PASSWORD}"
  "-Dbalance.timeout=${TIMEOUT}"
)

CLASSPATH="$(${ACCUMULO_HOME}/accumulo classpath)"
CLASSPATH="${CLASSPATH}:${DATAWAVE_ACCUMULO_EXTENSIONS_JAR}"

${JAVA_HOME}/bin/java ${WAREHOUSE_JAVA_OPTS} \
  "${SYS_PROPS[@]}" \
  -cp ${CLASSPATH} \
  datawave.ingest.table.balancer.WaitForBalance

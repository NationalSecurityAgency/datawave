#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/common.sh

createTempPem

SHUTDOWN_ENDPOINT=https://localhost:8743/executor/mgmt/shutdown

FOLDER="executorShutdown_$(date +%Y%m%d_%I%M%S.%N)"

mkdir $FOLDER
cd $FOLDER

echo "$(date): Shutting down query executor service"
echo "$(date): Shutting down query executor service" > shutdownSummary.txt
curl -s -D headers_0.txt -k -E ${TMP_PEM} -X POST \
    -H "Accept: application/json" \
    ${SHUTDOWN_ENDPOINT} -o shutdownResponse.json -w '%{http_code}\n' >> shutdownSummary.txt

echo "$(date): Query Executor service shutdown"
echo "$(date): Query Executor service shutdown" > shutdownSummary.txt

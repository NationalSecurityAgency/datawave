#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/common.sh

createTempPem

SHUTDOWN_ENDPOINT=https://localhost:8443/query/mgmt/shutdown

FOLDER="queryShutdown_$(date +%Y%m%d_%I%M%S.%N)"

mkdir $FOLDER
cd $FOLDER

echo "$(date): Shutting down query service"
echo "$(date): Shutting down query service" > shutdownSummary.txt
curl -s -D headers_0.txt -k -E ${TMP_PEM} -X POST \
    -H "Accept: application/json" \
    ${SHUTDOWN_ENDPOINT} -o shutdownResponse.json -w '%{http_code}\n' >> shutdownSummary.txt

echo "$(date): Query service shutdown"
echo "$(date): Query service shutdown" > shutdownSummary.txt

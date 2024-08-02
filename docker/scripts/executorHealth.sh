#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/common.sh

createTempPem

HEALTH_ENDPOINT=https://localhost:8743/executor/mgmt/health

FOLDER="executorHealth_$(date +%Y%m%d_%I%M%S.%N)"

mkdir $FOLDER
cd $FOLDER

echo "$(date): Getting query executor service health"
echo "$(date): Getting query executor service health" > healthSummary.txt
curl -s -D headers_0.txt -k -E ${TMP_PEM} \
    -H "Accept: application/json" \
    ${HEALTH_ENDPOINT} -o healthResponse.json -w '%{http_code}\n' >> healthSummary.txt

echo "$(date): Query Executor service health retrieved"
echo "$(date): Query Executor service health retrieved" > healthSummary.txt

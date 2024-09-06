#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/common.sh

createTempPem

HEALTH_ENDPOINT=https://localhost:8443/query/mgmt/health

FOLDER="queryHealth_$(date +%Y%m%d_%I%M%S.%N)"

mkdir $FOLDER
cd $FOLDER

echo "$(date): Getting query service health"
echo "$(date): Getting query service health" > healthSummary.txt
curl -s -D headers_0.txt -k -E ${TMP_PEM} \
    -H "Accept: application/json" \
    ${HEALTH_ENDPOINT} -o healthResponse.json -w '%{http_code}\n' >> healthSummary.txt

echo "$(date): Query service health retrieved"
echo "$(date): Query service health retrieved" > healthSummary.txt

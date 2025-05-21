#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/common.sh

createTempPem

EXECUTOR_ENDPOINT1=https://localhost:8743/executor/v1
EXECUTOR_ENDPOINT2=https://localhost:8843/executor/v1

FOLDER="executor_$(date +%Y%m%d_%I%M%S.%N)"

mkdir $FOLDER
cd $FOLDER

echo "$(date): polling connection factory for pool1"
curl -s -D headers_1.txt -k -E ${TMP_PEM} \
    -H "Accept: application/xml" \
    -H "Pool: $POOL" \
    ${EXECUTOR_ENDPOINT1}/Common/AccumuloConnectionFactory/stats -o connectionFactory1Response.txt -w '%{http_code}\n'
echo "$(date): polling connection factory for pool2"
curl -s -D headers_2.txt -k -E ${TMP_PEM} \
    -H "Accept: application/xml" \
    -H "Pool: $POOL" \
    ${EXECUTOR_ENDPOINT2}/Common/AccumuloConnectionFactory/stats -o connectionFactory2Response.txt -w '%{http_code}\n'

cd ../

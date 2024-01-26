#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/common.sh

createTempPem

MODIFICATION_ENDPOINT=https://localhost:9343/modification/v1

FOLDER="modification_$(date +%Y%m%d_%I%M%S.%N)"

mkdir $FOLDER
cd $FOLDER

echo "$(date): polling modification service for configurations"
curl -s -D headers_1.txt -k -E ${TMP_PEM} \
    -H "Accept: application/json" \
    ${MODIFICATION_ENDPOINT}/listConfigurations -o modificationConfigurationResponse.txt -w '%{http_code}\n'

echo "$(date): reloading modification service fields"
curl -s -D headers_2.txt -k -E ${TMP_PEM} \
    -H "Accept: application/json" \
    ${MODIFICATION_ENDPOINT}/reloadCache -o modificationReloadCacheResponse.txt -w '%{http_code}\n'

echo "$(date): polling modification service fields"
curl -s -D headers_2.txt -k -E ${TMP_PEM} \
    -H "Accept: application/json" \
    ${MODIFICATION_ENDPOINT}/getMutableFieldList -o modificationFieldListResponse.txt -w '%{http_code}\n'

cd ../

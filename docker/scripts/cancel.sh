#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/common.sh

createTempPem

echo "$(date): Canceling query"
curl -X POST -s -k -E ${TMP_PEM} ${DATAWAVE_ENDPOINT}/$1/cancel -w '%{http_code}\n'

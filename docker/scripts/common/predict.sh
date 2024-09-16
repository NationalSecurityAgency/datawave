#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source ${SCRIPT_DIR}/common/common.sh

POOL="${POOL:-pool1}"

# QUERY PARAMETERS
#QUERY_LOGIC='EventQuery'
#BEGIN='19660908 000000.000'
#END='20161002 235959.999'
#COLUMN_VISIBILITY='PUBLIC'
#QUERY='GENRES:[Action to Western]'
#QUERY_SYNTAX='LUCENE'
#AUTHS='PUBLIC,PRIVATE,BAR,FOO'
#QUERY_NAME='Developer Test Query'
#EXPAND_VALUES='true'

get_query_prediction () {
    while read_dom; do
        if [[ $ENTITY =~ 'Result' ]] && [[ ! $ENTITY =~ 'HasResults'  ]]; then
            echo $CONTENT
            break
        fi
    done
}

runPredict() {
    createTempPem

    FOLDER="predict_$(date +%Y%m%d_%I%M%S.%N)"

    mkdir $FOLDER
    cd $FOLDER

    SYSTEM_FROM=$(hostname)

    echo "$(date): Predicting query"
    echo "$(date): Predicting query" > querySummary.txt
    curl -s -D headers_0.txt -k -E ${TMP_PEM} \
        -H "Accept: application/xml" \
        -H "Pool: $POOL" \
        --data-urlencode "begin=${BEGIN}" \
        --data-urlencode "end=${END}" \
        --data-urlencode "columnVisibility=${COLUMN_VISIBILITY}" \
        --data-urlencode "query=${QUERY}" \
        --data-urlencode "query.syntax=${QUERY_SYNTAX}" \
        --data-urlencode "auths=${AUTHS}" \
        --data-urlencode "systemFrom=${SYSTEM_FROM}" \
        --data-urlencode "queryName=${QUERY_NAME}" \
        --data-urlencode "expand.values=${EXPAND_VALUES}" \
        ${DATAWAVE_ENDPOINT}/${QUERY_LOGIC}/predict -o predictResponse.txt -w '%{http_code}\n' >> querySummary.txt

    QUERY_PREDICTION=$(get_query_prediction < predictResponse.txt)

    echo "$(date): Received query prediction"
    echo "$(date): Received query prediction" >> querySummary.txt

    echo "$QUERY_PREDICTION"
    echo "$QUERY_PREDICTION" >> querySummary.txt

    cd ../
}

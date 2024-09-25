#!/bin/bash

source ${SCRIPT_DIR}/common/common.sh

PAUSE='false'
POOL="${POOL:-pool1}"
MAX_PAGES=100
QUERY_TYPE='lookupContent'

# QUERY PARAMETERS
#BEGIN='19660908 000000.000'
#END='20161002 235959.999'
#COLUMN_VISIBILITY='PUBLIC'
#UUID_TYPE='PAGE_TITLE'
#UUID='anarchism'
#QUERY='GENRES:[Action to Western]'
#QUERY_SYNTAX='LUCENE'
#AUTHS='PUBLIC,PRIVATE,BAR,FOO'
#QUERY_NAME='Developer Test Lookup UUID Query'

# Override common get_query_id
get_query_id () {
    while read_dom; do
        if [[ $ENTITY =~ 'QueryId' ]]; then
            echo $CONTENT
            break
        fi
    done
}

runLookupContent() {
    createTempPem

    FOLDER="${QUERY_TYPE}_$(date +%Y%m%d_%I%M%S.%N)"

    mkdir $FOLDER
    cd $FOLDER

    SYSTEM_FROM=$(hostname)

    echo "$(date): Running LookupUUID query"
    echo "$(date): Running LookupUUID query" > querySummary.txt
    curl -s -D headers_0.txt -X GET -k -E ${TMP_PEM} \
        -H "Accept: application/xml" \
        -H "Pool: $POOL" \
        --data-urlencode "begin=${BEGIN}" \
        --data-urlencode "end=${END}" \
        --data-urlencode "columnVisibility=${COLUMN_VISIBILITY}" \
        --data-urlencode "auths=${AUTHS}" \
        --data-urlencode "systemFrom=${SYSTEM_FROM}" \
        --data-urlencode "queryName=${QUERY_NAME}" \
        ${DATAWAVE_ENDPOINT}/lookupContentUUID/${UUID_TYPE}/${UUID} -o lookupResponse.xml -w '%{http_code}\n' >> querySummary.txt

    QUERY_ID=$(get_query_id < lookupResponse.xml)
    NUM_EVENTS=$(get_num_events < lookupResponse.xml)
    echo "$(date): Returned $NUM_EVENTS events"
    echo "$(date): Returned $NUM_EVENTS events" >> querySummary.txt

    echo "$(date): Finished running $QUERY_ID"
    echo "$(date): Finished running $QUERY_ID" >> querySummary.txt

    cd ../

    logMetrics
}

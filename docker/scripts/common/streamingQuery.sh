#!/bin/bash

source ${SCRIPT_DIR}/common/common.sh

PAUSE='false'
POOL="${POOL:-pool1}"
MAX_PAGES=100
QUERY_TYPE='streamingQuery'

# QUERY PARAMETERS
#QUERY_LOGIC='EventQuery'
#BEGIN='19660908 000000.000'
#END='20161002 235959.999'
#COLUMN_VISIBILITY='PUBLIC'
#QUERY='GENRES:[Action to Western]'
#QUERY_SYNTAX='LUCENE'
#AUTHS='PUBLIC,PRIVATE,BAR,FOO'
#QUERY_NAME='Developer Test Streaming Query'
#PAGE_SIZE='10'

# Override common get_query_id
get_query_id () {
    while read_dom; do
        if [[ $ENTITY =~ 'QueryId' ]]; then
            echo $CONTENT
            break
        fi
    done
}

# Override common get_num_events
get_num_events () {
    count=0
    while read_dom; do
        if [[ $ENTITY = 'ReturnedEvents' ]]; then
            count=$((count + CONTENT))
        fi
    done
    echo $count
}

runStreamingQuery() {
    createTempPem

    FOLDER="${QUERY_TYPE}_$(date +%Y%m%d_%I%M%S.%N)"

    mkdir $FOLDER
    cd $FOLDER

    SYSTEM_FROM=$(hostname)

    echo "$(date): Running streaming query"
    echo "$(date): Running streaming query" > querySummary.txt
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
        --data-urlencode "pagesize=${PAGE_SIZE}" \
        ${DATAWAVE_ENDPOINT}/${QUERY_LOGIC}/createAndExecute -o streamingResponse.xml -w '%{http_code}\n' >> querySummary.txt

    QUERY_ID=$(get_query_id < streamingResponse.xml)
    NUM_EVENTS=$(get_num_events < streamingResponse.xml)

    echo "$(date): Streaming results contained $NUM_EVENTS events"
    echo "$(date): Streaming results contained $NUM_EVENTS events" >> querySummary.txt

    cd ../

    logMetrics
}

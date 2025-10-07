#!/bin/bash

source ${SCRIPT_DIR}/common/common.sh

PAUSE='false'
POOL="${POOL:-pool1}"
MAX_PAGES=100
QUERY_TYPE='edge'

# QUERY PARAMETERS
#QUERY_LOGIC='EdgeQuery'
#BEGIN='19660908 000000.000'
#END='20161002 235959.999'
#COLUMN_VISIBILITY='PUBLIC'
#QUERY='SOURCE == 'Jerry Seinfeld''
#AUTHS='PUBLIC,PRIVATE,BAR,FOO'
#QUERY_NAME='Developer Test Edge Query'
#PAGE_SIZE='100'

get_num_edges () {
    declare -i count=0
    while read_dom; do
        if [[ $ENTITY = '/Edge' ]]; then
            count=$((count + 1))
        fi
    done
    echo $count
}

runEdgeQuery() {
    createTempPem

    FOLDER="${QUERY_TYPE}_$(date +%Y%m%d_%I%M%S.%N)"

    mkdir $FOLDER
    cd $FOLDER

    SYSTEM_FROM=$(hostname)

    echo "$(date): Creating query"
    echo "$(date): Creating query" > querySummary.txt
    curl -s -D headers_0.txt -k -E ${TMP_PEM} \
        -H "Accept: application/xml" \
        -H "Pool: $POOL" \
        --data-urlencode "begin=${BEGIN}" \
        --data-urlencode "end=${END}" \
        --data-urlencode "columnVisibility=${COLUMN_VISIBILITY}" \
        --data-urlencode "query=${QUERY}" \
        --data-urlencode "auths=${AUTHS}" \
        --data-urlencode "systemFrom=${SYSTEM_FROM}" \
        --data-urlencode "queryName=${QUERY_NAME}" \
        --data-urlencode "pagesize=${PAGE_SIZE}" \
        ${DATAWAVE_ENDPOINT}/${QUERY_LOGIC}/create -o createResponse.xml -w '%{http_code}\n' >> querySummary.txt

    i=1

    QUERY_ID=$(get_query_id < createResponse.xml)

    TOTAL_EVENTS=0
    TOTAL_PAGES=0

    while [ $i -gt 0 ] && [ $i -lt $MAX_PAGES ]; do
        echo "$(date): Requesting page $i for $QUERY_ID"
        echo "$(date): Requesting page $i for $QUERY_ID" >> querySummary.txt
        curl -s -D headers_$i.txt -q -k -E ${TMP_PEM} \
            -H "Accept: application/xml" \
        -H "Pool: $POOL" \
            ${DATAWAVE_ENDPOINT}/$QUERY_ID/next -o nextResponse_$i.xml -w '%{http_code}\n' >> querySummary.txt

        CONTINUE=`grep 'HTTP/2 200' headers_$i.txt`

        if [ -z "$CONTINUE" ]; then
            i=-1
        else
            NUM_EVENTS=$(get_num_edges < nextResponse_$i.xml)
            TOTAL_EVENTS=$((TOTAL_EVENTS + NUM_EVENTS))
            TOTAL_PAGES=$((TOTAL_PAGES + 1))
            echo "$(date): Page $i contained $NUM_EVENTS edges"
            echo "$(date): Page $i contained $NUM_EVENTS edges" >> querySummary.txt

            ((i++))
        fi

        if [ "$PAUSE" == "true" ]; then
            echo "press any key to continue"
            read -n 1
        fi
    done

    echo "$(date): Returned $TOTAL_PAGES pages"
    echo "$(date): Returned $TOTAL_PAGES pages" >> querySummary.txt

    echo "$(date): Returned $TOTAL_EVENTS events"
    echo "$(date): Returned $TOTAL_EVENTS events" >> querySummary.txt

    echo "$(date): Closing $QUERY_ID"
    echo "$(date): Closing $QUERY_ID" >> querySummary.txt
    # close the query
    curl -s -q -k -X POST -E ${TMP_PEM} \
        -H "Accept: application/xml" \
        -H "Pool: $POOL" \
        ${DATAWAVE_ENDPOINT}/$QUERY_ID/close -o closeResponse.xml -w '%{http_code}\n' >> querySummary.txt

    cd ../

    logMetrics
}

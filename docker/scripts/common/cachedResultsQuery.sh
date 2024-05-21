#!/bin/bash

source ${SCRIPT_DIR}/common/common.sh

PAUSE='false'
POOL="${POOL:-pool1}"
MAX_PAGES=100
QUERY_TYPE='cachedResultsQuery'

# QUERY PARAMETERS
#QUERY_LOGIC='EventQuery'
#BEGIN='19660908 000000.000'
#END='20161002 235959.999'
#COLUMN_VISIBILITY='PUBLIC'
#QUERY='GENRES:[Action to Western]'
#QUERY_SYNTAX='LUCENE'
#AUTHS='PUBLIC,PRIVATE,BAR,FOO'
#QUERY_NAME='Developer Test Query'
#PAGE_SIZE='10'

# Override common get_query_id
get_query_id () {
    while read_dom; do
        if [[ $ENTITY = 'QueryId' ]]; then
            echo $CONTENT
            break
        fi
    done
}

get_result () {
    while read_dom; do
        if [[ $ENTITY =~ 'Result' ]] && [[ ! $ENTITY =~ 'HasResults'  ]]; then
            echo $CONTENT
            break
        fi
    done
}

get_total_num_events () {
    while read_dom; do
        if [[ $ENTITY = 'TotalEvents' ]]; then
            echo $CONTENT
            break
        fi
    done
}

runCachedResultsQuery() {
    createTempPem
    
    FOLDER="${QUERY_TYPE}_$(date +%Y%m%d_%I%M%S.%N)"

    mkdir $FOLDER
    cd $FOLDER

    SYSTEM_FROM=$(hostname)

    echo "$(date): Defining query"
    echo "$(date): Defining query" > querySummary.txt
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
        ${DATAWAVE_ENDPOINT}/${QUERY_LOGIC}/define -o defineResponse.xml -w '%{http_code}\n' >> querySummary.txt

    QUERY_ID=$(get_result < defineResponse.xml)

    echo "$(date): Loading cached results"
    echo "$(date): Loading cached results" > querySummary.txt
    curl -s -D headers_1.txt -k -E ${TMP_PEM} \
        -H "Accept: application/xml" \
        -H "Pool: $POOL" \
        ${CACHEDRESULTS_ENDPOINT}/$QUERY_ID/load?alias=alias-${QUERY_ID} -o loadResponse.xml -w '%{http_code}\n' >> querySummary.txt

    VIEW_NAME=$(get_result < loadResponse.xml)

    echo "$(date): Creating the SQL query"
    echo "$(date): Creating the SQL query" > querySummary.txt
    curl -s -D headers_2.txt -k -X POST -E ${TMP_PEM} \
        -H "Accept: application/xml" \
        -H "Pool: $POOL" \
        --data-urlencode "fields=" \
        --data-urlencode "conditions=" \
        --data-urlencode "grouping=" \
        --data-urlencode "order=" \
        --data-urlencode "fixedFields=" \
        --data-urlencode "pagesize=10" \
        ${CACHEDRESULTS_ENDPOINT}/$VIEW_NAME/create -o createResponse.xml -w '%{http_code}\n' >> querySummary.txt

    METRICS_QUERY_ID=$(get_query_id < createResponse.xml)

    i=1
    TOTAL_NUM_EVENTS=0
    TOTAL_EVENTS=0
    TOTAL_PAGES=0

    while [ $i -gt 0 ] && [ $i -lt $MAX_PAGES ]; do
        if [ "$PAUSE" == "true" ]; then
            echo "press any key to continue"
            read -n 1
        fi

        echo "$(date): Requesting page $i for $VIEW_NAME"
        echo "$(date): Requesting page $i for $VIEW_NAME" >> querySummary.txt
        curl -s -D headers_$((i + 3)).txt -k -E ${TMP_PEM} \
            -H "Accept: application/xml" \
        -H "Pool: $POOL" \
            "${CACHEDRESULTS_ENDPOINT}/$VIEW_NAME/getRows?rowBegin=$((TOTAL_PAGES * PAGE_SIZE + 1))&rowEnd=$(((TOTAL_PAGES + 1) * PAGE_SIZE))" -o getRowsResponse_$i.xml -w '%{http_code}\n' >> querySummary.txt

        CONTINUE=`grep 'HTTP/2 200' headers_$((i + 3)).txt`

        if [ -z "$CONTINUE" ]; then
            i=-1
        else
            NUM_EVENTS=$(get_num_events < getRowsResponse_$i.xml)
            TOTAL_NUM_EVENTS=$(get_total_num_events < getRowsResponse_$i.xml)
            TOTAL_EVENTS=$((TOTAL_EVENTS + NUM_EVENTS))
            TOTAL_PAGES=$((TOTAL_PAGES + 1))
            echo "$(date): Page $i contained $NUM_EVENTS events"
            echo "$(date): Page $i contained $NUM_EVENTS events" >> querySummary.txt

            if [ $TOTAL_EVENTS -ge $TOTAL_NUM_EVENTS ]; then
                i=-1
            else
                ((i++))
            fi
        fi
    done

    echo "$(date): Returned $TOTAL_PAGES pages"
    echo "$(date): Returned $TOTAL_PAGES pages" >> querySummary.txt

    echo "$(date): Returned $TOTAL_EVENTS events"
    echo "$(date): Returned $TOTAL_EVENTS events" >> querySummary.txt

    cd ../

    logMetrics
}

#!/bin/bash

source ${SCRIPT_DIR}/common/common.sh

PAUSE='false'
POOL="${POOL:-pool1}"
MAX_PAGES=100
QUERY_TYPE='oozieQuery'

# QUERY PARAMETERS
#WORKFLOW='OozieJob'
#COLUMN_VISIBILITY='PUBLIC'

get_job_status () {
    while read_dom; do
        if [[ $ENTITY =~ 'JobExecution' ]]; then
            if [[ $ENTITY =~ 'state="DEFINED"' ]]; then
                echo "DEFINED"
                break
            elif [[ $ENTITY =~ 'state="SUBMITTED"' ]]; then
                echo "SUBMITTED"
                break
            elif [[ $ENTITY =~ 'state="RUNNING"' ]]; then
                echo "RUNNING"
                break
            elif [[ $ENTITY =~ 'state="SUCCEEDED"' ]]; then
                echo "SUCCEEDED"
                break
            elif [[ $ENTITY =~ 'state="CLOSED"' ]]; then
                echo "CLOSED"
                break
            elif [[ $ENTITY =~ 'state="CANCELED"' ]]; then
                echo "CANCELED"
                break
            elif [[ $ENTITY =~ 'state="FAILED"' ]]; then
                echo "FAILED"
                break
            fi
        fi
    done
}

# Override common get_num_events
get_num_events () {
    local EVENTS=0
    while read_dom; do
        if [[ $ENTITY = 'ReturnedEvents' ]] || [[ $ENTITY = 'returnedEvents' ]]; then
            EVENTS=$((EVENTS + CONTENT))
        fi
    done
    echo $EVENTS
}

# Override common logMetrics
logMetrics () {
    if [ ! -z "$JOB_ID" ]; then
        mv $FOLDER ${QUERY_TYPE}_${JOB_ID}

        echo "$(date): Job status available at: ${DATAWAVE_ENDPOINT}/mapreduce/${JOB_ID}/list"
        echo "$(date): Job status available at: ${DATAWAVE_ENDPOINT}/mapreduce/${JOB_ID}/list" >> ${QUERY_TYPE}_${JOB_ID}/querySummary.txt
    fi
}

runOozieQuery() {
    createTempPem

    FOLDER="${QUERY_TYPE}_$(date +%Y%m%d_%I%M%S.%N)"

    mkdir $FOLDER
    cd $FOLDER

    SYSTEM_FROM=$(hostname)

    echo "$(date): Submitting oozie query"
    echo "$(date): Submitting oozie query" >> querySummary.txt

    # To write the output to a table, add the following parameter
    # --data-urlencode "outputTableName=ResultsTable" \

    curl -s -D headers_1.txt -k -E ${TMP_PEM} \
        -H "Accept: application/xml" \
        -H "Pool: $POOL" \
        --data-urlencode "workFlow=${WORKFLOW}" \
        --data-urlencode "columnVisibility=${COLUMN_VISIBILITY}" \
        ${MAPREDUCE_ENDPOINT}/oozieSubmit -o submitResponse.xml -w '%{http_code}\n' >> querySummary.txt

    JOB_ID=$(get_query_id < submitResponse.xml)

    ATTEMPTS=6
    ATTEMPT=1
    TIMEOUT=20

    JOB_STATUS="DEFINED"

    while [ $ATTEMPT -le $ATTEMPTS ]; do
        echo "$(date): Checking oozie query status (Attempt ${ATTEMPT}/${ATTEMPTS})"
        echo "$(date): Checking oozie query status (Attempt ${ATTEMPT}/${ATTEMPTS})" >> querySummary.txt

        curl -s -k -E ${TMP_PEM} \
            ${MAPREDUCE_ENDPOINT}/${JOB_ID}/list -o listResponse.xml -w '%{http_code}\n' >> querySummary.txt

        JOB_STATUS=$(get_job_status < listResponse.xml)

        echo "$(date): Job Status: $JOB_STATUS"
        echo "$(date): Job Status: $JOB_STATUS" >> querySummary.txt

        if [ "$JOB_STATUS" != "DEFINED" ] && [ "$JOB_STATUS" != "SUBMITTED" ] && [ "$JOB_STATUS" != "RUNNING" ]; then
            break;
        fi

        if [ $ATTEMPT -lt $ATTEMPTS ]; then
            sleep ${TIMEOUT}
            ((ATTEMPT++))
        fi
    done

    TOTAL_EVENTS=0
    TOTAL_FILES=0
    if [ "$JOB_STATUS" == "SUCCEEDED" ]; then
        echo "$(date): Downloading results.tar"
        echo "$(date): Downloading results.tar" >> querySummary.txt

        curl -s -k -E ${TMP_PEM} \
            ${MAPREDUCE_ENDPOINT}/${JOB_ID}/getAllFiles -o results.tar -w '%{http_code}\n' >> querySummary.txt
        
        tar -xf results.tar

        cd ${JOB_ID}

        for f in $(ls)
        do
            NUM_EVENTS=$(get_num_events < $f)
            TOTAL_EVENTS=$((TOTAL_EVENTS + NUM_EVENTS))
            TOTAL_FILES=$((TOTAL_FILES + 1))

            echo "$(date):   $f contained $NUM_EVENTS events"
            echo "$(date):   $f contained $NUM_EVENTS events" >> querySummary.txt
        done

        cd ..
    fi

    echo "$(date): Returned $TOTAL_FILES files"
    echo "$(date): Returned $TOTAL_FILES files" >> querySummary.txt

    echo "$(date): Returned $TOTAL_EVENTS events"
    echo "$(date): Returned $TOTAL_EVENTS events" >> querySummary.txt

    cd ../

    logMetrics
}
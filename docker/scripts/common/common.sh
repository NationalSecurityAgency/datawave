#!/bin/bash

WEBSERVICE="${WEBSERVICE:-false}"

if [ "$WEBSERVICE" = true ]; then
    DATAWAVE_ENDPOINT=https://localhost:9443/DataWave/Query
    CACHEDRESULTS_ENDPOINT=https://localhost:9443/DataWave/CachedResults
    MAPREDUCE_ENDPOINT=https://localhost:9443/DataWave/MapReduce
else
    DATAWAVE_ENDPOINT=https://localhost:8443/query/v1/query
    CACHEDRESULTS_ENDPOINT=https://localhost:8443/query/v1/cachedresults
    MAPREDUCE_ENDPOINT=https://localhost:8443/query/v1/mapreduce
fi

METRICS_ENDPOINT=https://localhost:8543/querymetric/v1

createTempPem() {
    # use the test user pkcs12 cert
    P12_KEYSTORE=${SCRIPT_DIR}/../pki/testUser.p12
    P12_KEYSTORE_PASS=ChangeIt

    TMP_DIR=/dev/shm
    TMP_PEM="$TMP_DIR/testUser-$$-pem"

    sh -c "while kill -0 $$ 2>/dev/null; do sleep 1; done; rm -f '${TMP_P12}' '${TMP_PEM}'" &

    function needsPassphrase() {
        [ -z "${P12_KEYSTORE_PASS}" ]
    }

    function getFromCliPrompt() {
        read -s -p "Passphrase for ${P12_KEYSTORE}: " P12_KEYSTORE_PASS && echo 1>&2
    }

    needsPassphrase && getFromCliPrompt

    params=""
    opensslVersion3="$( openssl version | awk '{ print $2 }' | grep -E ^3\. )"
    if [ ! -z "$opensslVersion3" ]; then
        params="-provider legacy -provider default"
    fi

    # Create one-time passphrase and certificate
    OLD_UMASK=$(umask)
    umask 0277
    export P12_KEYSTORE_PASS
    openssl pkcs12 ${params} \
        -in ${P12_KEYSTORE} -passin env:P12_KEYSTORE_PASS \
        -out ${TMP_PEM} -nodes 2>/dev/null
    opensslexit=$?
    umask $OLD_UMASK
    [ $opensslexit = 0 ] || echo "Error creating temporary certificate file"
}

read_dom () {
    local IFS=\>
    read -d \< ENTITY CONTENT
}

get_query_id () {
    while read_dom; do
        if [[ $ENTITY =~ 'Result' ]] && [[ ! $ENTITY =~ 'HasResults' ]]; then
            echo $CONTENT
            break
        fi
    done
}

get_num_events () {
    while read_dom; do
        if [[ $ENTITY = 'ReturnedEvents' ]]; then
            echo $CONTENT
            break
        fi
    done
}

logMetrics () {
    if [ ! -z "$QUERY_ID" ]; then
        mv ${FOLDER} ${QUERY_TYPE}_${QUERY_ID}

        echo "$(date): Getting metrics for ${QUERY_ID}"
        echo "$(date): Getting metrics for ${QUERY_ID}" >> ${QUERY_TYPE}_${QUERY_ID}/querySummary.txt

        echo "$(date): Metrics available at: ${METRICS_ENDPOINT}/id/${QUERY_ID}"
        echo "$(date): Metrics available at: ${METRICS_ENDPOINT}/id/${QUERY_ID}" >> ${QUERY_TYPE}_${QUERY_ID}/querySummary.txt
    fi
}

printLine() {
    echo "$( printGreen "********************************************************************************************************" )"
}

printRed() {
    echo -ne "${DW_COLOR_RED}${1}${DW_COLOR_RESET}"
}

printGreen() {
    echo -ne "${DW_COLOR_GREEN}${1}${DW_COLOR_RESET}"
}

setPrintColors() {
    DW_COLOR_RED="\033[31m"
    DW_COLOR_GREEN="\033[32m"
    DW_COLOR_RESET="\033[m"
}

setTestLabels() {
    LABEL_PASS="$( printGreen PASSED )"
    LABEL_FAIL="$( printRed FAILED )"
}

printTestStatus() {
    elapsed_time=$(echo "scale=3; ($2 - $1) / 1000000000" | bc)
    echo
    echo "Test Total Time: $elapsed_time seconds"
    echo "Test Status: $3"
    echo
}
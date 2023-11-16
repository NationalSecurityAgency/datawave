#!/bin/bash

DATAWAVE_ENDPOINT=https://localhost:8443/query/v1/query
METRICS_ENDPOINT=https://localhost:8543/querymetric/v1

PAUSE='false'

POOL="${POOL:-pool1}"

MAX_PAGES=100

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

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

read_dom () {
    local IFS=\>
    read -d \< ENTITY CONTENT
}

get_query_id () {
    while read_dom; do
        if [[ $ENTITY =~ 'QueryId' ]]; then
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

FOLDER="lookupContent_$(date +%Y%m%d_%I%M%S.%N)"

mkdir $FOLDER
cd $FOLDER

SYSTEM_FROM=$(hostname)

echo "$(date): Running LookupContentUUID query"
echo "$(date): Running LookupContentUUID query" > querySummary.txt
curl -s -D headers_0.txt -X GET -k -E ${TMP_PEM} \
    -H "Accept: application/xml" \
    -H "Pool: $POOL" \
    --data-urlencode "begin=19660908 000000.000" \
    --data-urlencode "end=20161002 235959.999" \
    --data-urlencode "columnVisibility=PUBLIC" \
    --data-urlencode "auths=PUBLIC,PRIVATE,BAR,FOO" \
    --data-urlencode "systemFrom=$SYSTEM_FROM" \
    --data-urlencode "queryName=Developer Test Lookup Content UUID Query" \
    ${DATAWAVE_ENDPOINT}/lookupContentUUID/PAGE_TITLE/anarchism -o lookupResponse.xml -w '%{http_code}\n' >> querySummary.txt

QUERY_ID=$(get_query_id < lookupResponse.xml)
NUM_EVENTS=$(get_num_events < lookupResponse.xml)
echo "$(date): Returned $NUM_EVENTS events"
echo "$(date): Returned $NUM_EVENTS events" >> querySummary.txt

echo "$(date): Finished running $QUERY_ID"
echo "$(date): Finished running $QUERY_ID" >> querySummary.txt

cd ../

if [ ! -z "$QUERY_ID" ]; then
    mv $FOLDER lookupContent_$QUERY_ID

    echo "$(date): Getting metrics for $QUERY_ID"
    echo "$(date): Getting metrics for $QUERY_ID" >> lookupContent_$QUERY_ID/querySummary.txt

    echo "$(date): Metrics available at: ${METRICS_ENDPOINT}/id/$QUERY_ID"
    echo "$(date): Metrics available at: ${METRICS_ENDPOINT}/id/$QUERY_ID" >> lookupContent_$QUERY_ID/querySummary.txt
fi
